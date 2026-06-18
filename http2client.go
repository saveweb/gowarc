package warc

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	http "github.com/saveweb/fhttp"
	"github.com/saveweb/fhttp/httputil"
	tls_client "github.com/saveweb/tls-client"
	"github.com/google/uuid"
	"github.com/saveweb/gowarc/pkg/spooledtempfile"
	"golang.org/x/sync/errgroup"

	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

type http2Client struct {
	client    *CustomHTTPClient
	tlsClient tls_client.HttpClient
	enableH3  bool
}

func newHTTP2Client(client *CustomHTTPClient, enableH3 bool, forceH3 bool) (*http2Client, error) {
	opts := []tls_client.HttpClientOption{
		tls_client.WithClientProfile(client.tlsProfile.clientProfile),
		tls_client.WithRandomTLSExtensionOrder(),
		tls_client.WithNotFollowRedirects(),
		tls_client.WithTimeoutSeconds(30),
	}

	if client.insecureSkipVerifyCerts {
		opts = append(opts, tls_client.WithInsecureSkipVerify())
	}

	if forceH3 {
		opts = append(opts, tls_client.WithForceH3())
	} else if !enableH3 {
		opts = append(opts, tls_client.WithDisableHttp3())
	}

	to := &tls_client.TransportOptions{
		MaxIdleConns:        client.keepAliveMaxIdle,
		MaxIdleConnsPerHost: client.keepAliveMaxIdle,
		DisableCompression:  true,
	}
	if client.keepAliveIdleTimeout > 0 {
		idle := client.keepAliveIdleTimeout
		to.IdleConnTimeout = &idle
	}
	opts = append(opts, tls_client.WithTransportOptions(to))

	if client.disableIPv4 {
		opts = append(opts, tls_client.WithDisableIPV4())
	}
	if client.disableIPv6 {
		opts = append(opts, tls_client.WithDisableIPV6())
	}

	tc, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
	if err != nil {
		return nil, fmt.Errorf("http2client: creating tls-client: %w", err)
	}

	return &http2Client{
		client:    client,
		tlsClient: tc,
		enableH3:  enableH3,
	}, nil
}

func (c *http2Client) Close() {
	c.tlsClient.CloseIdleConnections()
}

type h2BodyWrapper struct {
	inner      io.Reader
	warcWriter io.Writer
	done       chan struct{}
	once       sync.Once
	n          int64
}

func (r *h2BodyWrapper) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		r.n += int64(n)
		if r.warcWriter != nil {
			r.warcWriter.Write(p[:n])
		}
	}
	if err != nil {
		r.once.Do(func() { close(r.done) })
	}
	return n, err
}

func (c *http2Client) Do(ctx context.Context, req *http.Request) (*http.Response, *protocolInfo, error) {
	scheme := "https"
	if req.URL != nil && req.URL.Scheme == "http" {
		scheme = "http"
	}

	reqTemp, err := spooledtempfile.NewSpooledTempFile("warc-req", c.client.TempDir)
	if err != nil {
		return nil, nil, fmt.Errorf("http2client: creating req temp file: %w", err)
	}
	respTemp, err := spooledtempfile.NewSpooledTempFile("warc-resp", c.client.TempDir)
	if err != nil {
		reqTemp.Close()
		return nil, nil, fmt.Errorf("http2client: creating resp temp file: %w", err)
	}

	reqBytes, err := httputil.DumpRequestOut(req, req.Body != nil && req.ContentLength > 0)
	if err != nil {
		reqTemp.Close()
		respTemp.Close()
		return nil, nil, fmt.Errorf("http2client: dumping request: %w", err)
	}
	reqTemp.Write(reqBytes)

	if req.Body != nil && req.GetBody != nil {
		body, berr := req.GetBody()
		if berr == nil && body != nil {
			req.Body = body
		}
	}

	bodyDone := make(chan struct{})

	resp, err := c.tlsClient.Do(req)
	if err != nil {
		reqTemp.Close()
		respTemp.Close()
		return nil, nil, fmt.Errorf("http2client: doing request: %w", err)
	}

	statusText := resp.Status
	if idx := strings.IndexByte(statusText, ' '); idx >= 0 {
		statusText = statusText[idx+1:]
	}
	statusLine := fmt.Sprintf("HTTP/1.1 %d %s\r\n", resp.StatusCode, statusText)
	io.WriteString(respTemp, statusLine)

	var headerBuf bytes.Buffer
	for k, vv := range resp.Header {
		for _, v := range vv {
			headerBuf.WriteString(k)
			headerBuf.WriteString(": ")
			headerBuf.WriteString(v)
			headerBuf.WriteString("\r\n")
		}
	}
	headerBuf.WriteString("\r\n")
	respTemp.Write(headerBuf.Bytes())

	bodyWrapper := &h2BodyWrapper{
		inner:      resp.Body,
		warcWriter: respTemp,
		done:       bodyDone,
	}

	var bodyReader io.Reader = bodyWrapper

	if c.client.DecompressBody {
		switch strings.ToLower(resp.Header.Get("Content-Encoding")) {
		case "gzip":
			gzReader, gerr := gzip.NewReader(bodyReader)
			if gerr != nil {
				return nil, nil, fmt.Errorf("creating gzip reader: %w", gerr)
			}
			bodyReader = gzReader
		case "deflate":
			zlibReader, zerr := zlib.NewReader(bodyReader)
			if zerr != nil {
				return nil, nil, fmt.Errorf("creating deflate reader: %w", zerr)
			}
			bodyReader = zlibReader
		case "zstd":
			zstdReader, serr := zstd.NewReader(bodyReader)
			if serr != nil {
				return nil, nil, fmt.Errorf("creating zstd reader: %w", serr)
			}
			bodyReader = zstdReader.IOReadCloser()
		}
	}

	resp.Body = &h2ReadCloser{reader: bodyReader, bodyWrapper: bodyWrapper}

	proto := "h2"
	if resp.Proto == "HTTP/3.0" || resp.Proto == "HTTP/3" {
		proto = "h3"
	}
	pi := c.extractProtocolInfo(req.URL.Host, proto)

	c.client.WaitGroup.Add(1)
	go c.writeWARCFromConnection(ctx, scheme, reqTemp, respTemp, resp, bodyDone, pi)

	return resp, pi, nil
}

type h2ReadCloser struct {
	reader      io.Reader
	bodyWrapper *h2BodyWrapper
}

func (r *h2ReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *h2ReadCloser) Close() error {
	r.bodyWrapper.once.Do(func() { close(r.bodyWrapper.done) })
	return nil
}

func (c *http2Client) extractProtocolInfo(host string, proto string) *protocolInfo {
	pi := &protocolInfo{
		Protocol: proto,
	}

	addr := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		addr = h
	}

	connInfo := c.tlsClient.GetConnectionInfo(host)
	if connInfo == nil {
		if _, p, err := net.SplitHostPort(host); err == nil {
			connInfo = c.tlsClient.GetConnectionInfo(net.JoinHostPort(addr, p))
		}
	}
	if connInfo == nil {
		connInfo = c.tlsClient.GetConnectionInfo(net.JoinHostPort(addr, "443"))
	}
	if connInfo == nil {
		connInfo = c.tlsClient.GetConnectionInfo(net.JoinHostPort(addr, "80"))
	}

	if connInfo != nil {
		pi.TLSVersion = connInfo.TLSVersion
		pi.CipherSuite = connInfo.CipherSuite
		if connInfo.NegotiatedProtocol != "" {
			pi.Protocol = connInfo.NegotiatedProtocol
		}
		if connInfo.RemoteAddr != nil {
			pi.RemoteAddr = connInfo.RemoteAddr
			switch a := connInfo.RemoteAddr.(type) {
			case *net.TCPAddr:
				pi.DNSAddresses = []net.IP{a.IP}
			case *net.UDPAddr:
				pi.DNSAddresses = []net.IP{a.IP}
			}
		}
	}

	return pi
}

func (c *http2Client) writeWARCFromConnection(ctx context.Context, scheme string, reqTemp, respTemp spooledtempfile.ReadWriteSeekCloser, resp *http.Response, bodyDone chan struct{}, pi *protocolInfo) {
	defer c.client.WaitGroup.Done()

	select {
	case <-bodyDone:
	case <-ctx.Done():
		return
	}

	var feedbackChan chan struct{}
	batchSent := false
	if ctx.Value(ContextKeyFeedback) != nil {
		feedbackChan = ctx.Value(ContextKeyFeedback).(chan struct{})
		defer func() {
			if !batchSent {
				close(feedbackChan)
			}
		}()
	}

	var (
		batch           = NewRecordBatch(feedbackChan)
		recordChan      = make(chan *Record, 2)
		recordIDs       []string
		errs            = errgroup.Group{}
		targetURIReqCh  = make(chan string, 1)
		targetURIRespCh = make(chan string, 1)
	)

	errs.Go(func() error {
		return readRequestFromTempShared(ctx, scheme, c.client, reqTemp, targetURIReqCh, recordChan)
	})

	errs.Go(func() error {
		return readResponseFromTempShared(ctx, c.client, respTemp, targetURIReqCh, targetURIRespCh, recordChan)
	})

	readErr := errs.Wait()
	close(recordChan)

	if readErr != nil {
		if errors.Is(readErr, errDiscarded) {
			for record := range recordChan {
				record.Content.Close()
			}
			return
		}

		c.client.ErrChan <- &Error{
			Err:  readErr,
			Func: "http2client.writeWARCFromConnection",
		}

		for record := range recordChan {
			record.Content.Close()
		}
		return
	}

	for record := range recordChan {
		select {
		case <-ctx.Done():
			return
		default:
			recordIDs = append(recordIDs, uuid.NewString())
			batch.Records = append(batch.Records, record)
		}
	}

	if len(batch.Records) != 2 {
		c.client.ErrChan <- &Error{
			Err:  fmt.Errorf("warc: expected 2 records, got %d", len(batch.Records)),
			Func: "http2client.writeWARCFromConnection",
		}
		for _, record := range batch.Records {
			record.Content.Close()
		}
		return
	}

	if batch.Records[0].Header.Get("WARC-Type") != "response" {
		slicesReverse(batch.Records)
	}

	var warcTargetURI string
	select {
	case recv, ok := <-targetURIRespCh:
		if !ok {
			c.client.ErrChan <- &Error{
				Err:  fmt.Errorf("target URI channel closed"),
				Func: "http2client.writeWARCFromConnection",
			}
			return
		}
		warcTargetURI = recv
	case <-ctx.Done():
		return
	}

	for i, r := range batch.Records {
		select {
		case <-ctx.Done():
			return
		default:
			if pi.RemoteAddr != nil {
				switch addr := pi.RemoteAddr.(type) {
				case *net.TCPAddr:
					r.Header.Set("WARC-IP-Address", addr.IP.String())
				case *net.UDPAddr:
					r.Header.Set("WARC-IP-Address", addr.IP.String())
				}
			}

			r.Header.Add("WARC-Protocol", pi.ProtocolWARCValue())
			if tlsVal := pi.TLSVersionWARCValue(); tlsVal != "" {
				r.Header.Add("WARC-Protocol", tlsVal)
			}
			if pi.CipherSuite != 0 {
				r.Header.Set("WARC-Cipher-Suite", tlsCipherSuiteName(pi.CipherSuite))
			}
			if len(pi.DNSAddresses) > 0 {
				var ips []string
				for _, ip := range pi.DNSAddresses {
					ips = append(ips, ip.String())
				}
				r.Header.Set("WARC-DNS-Resolved-IP", strings.Join(ips, ","))
			}

			r.Header.Set("WARC-Record-ID", "<urn:uuid:"+recordIDs[i]+">")

			if i == len(recordIDs)-1 {
				r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[0]+">")
			} else {
				r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[1]+">")
			}

			r.Header.Set("WARC-Target-URI", warcTargetURI)

			if _, seekErr := r.Content.Seek(0, 0); seekErr != nil {
				c.client.ErrChan <- &Error{Err: seekErr, Func: "http2client.writeWARCFromConnection"}
				return
			}

			digest, err := GetDigest(r.Content, c.client.DigestAlgorithm)
			if err != nil {
				c.client.ErrChan <- &Error{Err: err, Func: "http2client.writeWARCFromConnection"}
				return
			}

			r.Header.Set("WARC-Block-Digest", digest)
			r.Header.Set("Content-Length", strconv.Itoa(getContentLength(r.Content)))

			if c.client.dedupeOptions.LocalDedupe {
				if r.Header.Get("WARC-Type") == "response" && !slicesContains(emptyPayloadDigests, r.Header.Get("WARC-Payload-Digest")) {
					captureTime, timeConversionErr := time.Parse(time.RFC3339, batch.CaptureTime)
					if timeConversionErr != nil {
						c.client.ErrChan <- &Error{Err: timeConversionErr, Func: "http2client.writeWARCFromConnection.timeConversionErr"}
						return
					}
					c.client.dedupeHashTable.Set(r.Header.Get("WARC-Payload-Digest"), revisitRecord{
						responseUUID: recordIDs[i],
						size:         getContentLength(r.Content),
						targetURI:    warcTargetURI,
						date:         captureTime,
					})
				}
			}
		}
	}

	select {
	case c.client.WARCWriter <- batch:
		batchSent = true
	case <-ctx.Done():
		return
	}
}

var _ = (protocolClient)((*http2Client)(nil))
