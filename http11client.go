package warc

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	utls "github.com/bogdanfinn/utls"
	"github.com/google/uuid"
	http "github.com/saveweb/fhttp"
	"github.com/saveweb/gowarc/pkg/spooledtempfile"
	"golang.org/x/sync/errgroup"

	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

type http11Client struct {
	client          *CustomHTTPClient
	pool            *connPool
	enableKeepAlive bool
}

func newHTTP11Client(client *CustomHTTPClient) *http11Client {
	maxIdle := 10
	idleTimeout := 90 * time.Second
	if client.keepAliveMaxIdle > 0 {
		maxIdle = client.keepAliveMaxIdle
	}
	if client.keepAliveIdleTimeout > 0 {
		idleTimeout = client.keepAliveIdleTimeout
	}

	dialer := newCustomDialer(client, client.dialTimeout, client.dnsRecordsTTL, client.dnsResolutionTimeout,
		client.dnsCacheSize, client.dnsServers, client.dnsFallback, client.dnsConcurrency,
		client.disableIPv4, client.disableIPv6)

	if client.tlsProfile != nil {
		dialer.tlsProfile = client.tlsProfile
	} else {
		dialer.tlsProfile = DefaultTLSProfile()
	}

	return &http11Client{
		client:          client,
		pool:            newConnPool(dialer, maxIdle, idleTimeout),
		enableKeepAlive: client.enableKeepAlive,
	}
}

func (c *http11Client) Close() {
	c.pool.closeAll()
}

func (c *http11Client) extractProtocolInfo(pc *pooledConn, scheme string) *protocolInfo {
	pi := &protocolInfo{
		Protocol:   "http/1.1",
		RemoteAddr: pc.conn.RemoteAddr(),
	}

	if scheme == "https" {
		if tc, ok := pc.conn.(*utls.UConn); ok {
			state := tc.ConnectionState()
			pi.TLSVersion = state.Version
			pi.CipherSuite = state.CipherSuite
		}
	}

	return pi
}

func (c *http11Client) Do(ctx context.Context, req *http.Request) (*http.Response, *protocolInfo, error) {
	scheme := "http"
	if req.URL != nil && req.URL.Scheme == "https" {
		scheme = "https"
	}
	host := req.URL.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		if scheme == "https" {
			host = net.JoinHostPort(host, "443")
		} else {
			host = net.JoinHostPort(host, "80")
		}
	}

	if c.enableKeepAlive {
		if req.Header.Get("Connection") == "" {
			req.Header.Set("Connection", "keep-alive")
		}
	} else {
		req.Header.Set("Connection", "close")
	}

	if req.Header.Get("User-Agent") == "" && c.client.defaultUserAgent != "" {
		req.Header.Set("User-Agent", c.client.defaultUserAgent)
	}

	pc, err := c.pool.getOrCreate(ctx, host, scheme)
	if err != nil {
		return nil, nil, err
	}

	reqTemp, err := spooledtempfile.NewSpooledTempFile("warc-req", c.client.TempDir)
	if err != nil {
		pc.Close()
		return nil, nil, fmt.Errorf("http11client: creating req temp file: %w", err)
	}
	respTemp, err := spooledtempfile.NewSpooledTempFile("warc-resp", c.client.TempDir)
	if err != nil {
		pc.Close()
		reqTemp.Close()
		return nil, nil, fmt.Errorf("http11client: creating resp temp file: %w", err)
	}

	bodyDone := make(chan struct{})

	err = req.Write(io.MultiWriter(pc.conn, reqTemp))
	if err != nil {
		pc.Close()
		reqTemp.Close()
		respTemp.Close()
		return nil, nil, fmt.Errorf("http11client: writing request: %w", err)
	}

	resp, err := c.readResponse(ctx, pc, req, respTemp, bodyDone)
	if err != nil {
		pc.Close()
		reqTemp.Close()
		respTemp.Close()
		return nil, nil, fmt.Errorf("http11client: reading response: %w", err)
	}

	keepAlive := c.enableKeepAlive && !resp.Close &&
		!strings.EqualFold(resp.Header.Get("Connection"), "close")

	pi := c.extractProtocolInfo(pc, scheme)

	c.client.WaitGroup.Add(1)
	go c.writeWARCFromConnection(ctx, reqTemp, respTemp, scheme, pc, keepAlive, resp, bodyDone, pi)

	resp.Request = req
	return resp, pi, nil
}

func (c *http11Client) readResponse(ctx context.Context, pc *pooledConn, req *http.Request, respTemp spooledtempfile.ReadWriteSeekCloser, bodyDone chan struct{}) (*http.Response, error) {
	tp := textproto.NewReader(pc.br)

	statusLine, err := tp.ReadLine()
	if err != nil {
		return nil, fmt.Errorf("reading status line: %w", err)
	}
	io.WriteString(respTemp, statusLine+"\r\n")

	proto, status, statusCode, err := parseStatusLine(statusLine)
	if err != nil {
		return nil, err
	}

	var rawHeaderBuf bytes.Buffer
	mimeHeader := make(http.Header)
	for {
		line, err := tp.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("reading headers: %w", err)
		}
		if line == "" {
			break
		}

		for {
			peek, err := pc.br.Peek(1)
			if err != nil || (peek[0] != ' ' && peek[0] != '\t') {
				break
			}
			cont, err := tp.ReadLine()
			if err != nil {
				break
			}
			line += "\r\n " + cont
		}

		rawHeaderBuf.WriteString(line)
		rawHeaderBuf.WriteString("\r\n")

		colonIdx := strings.IndexByte(line, ':')
		if colonIdx < 0 {
			continue
		}
		key := textproto.CanonicalMIMEHeaderKey(line[:colonIdx])
		value := strings.TrimLeft(line[colonIdx+1:], " \t")
		mimeHeader.Add(key, value)
	}
	rawHeaderBuf.WriteString("\r\n")
	respTemp.Write(rawHeaderBuf.Bytes())

	resp := &http.Response{
		Status:     status,
		StatusCode: statusCode,
		Proto:      proto,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     mimeHeader,
	}

	if proto == "HTTP/1.0" {
		resp.ProtoMinor = 0
	}

	resp.Close = shouldClose(resp.ProtoMinor, resp.Header)

	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	resp.ContentLength = contentLength

	transferEncoding := resp.Header["Transfer-Encoding"]
	resp.TransferEncoding = transferEncoding

	var bodyReader io.Reader

	if isChunked(transferEncoding) {
		cr := newChunkedBodyReader(pc.br, respTemp)
		cr.conn = pc.conn
		cr.readDeadline = c.client.ConnReadDeadline
		bodyReader = cr
	} else if contentLength > 0 {
		lr := newLimitedBodyReader(pc.br, respTemp, contentLength)
		lr.conn = pc.conn
		lr.readDeadline = c.client.ConnReadDeadline
		bodyReader = lr
	} else if contentLength == 0 {
		bodyReader = bytes.NewReader(nil)
	} else {
		if resp.Close {
			eofr := newEOFBodyReader(pc.br, respTemp)
			eofr.conn = pc.conn
			eofr.readDeadline = c.client.ConnReadDeadline
			bodyReader = eofr
		} else {
			bodyReader = bytes.NewReader(nil)
		}
	}

	if c.client.DecompressBody {
		switch strings.ToLower(resp.Header.Get("Content-Encoding")) {
		case "gzip":
			gzReader, err := gzip.NewReader(bodyReader)
			if err != nil {
				return nil, fmt.Errorf("creating gzip reader: %w", err)
			}
			bodyReader = gzReader
		case "deflate":
			zlibReader, err := zlib.NewReader(bodyReader)
			if err != nil {
				return nil, fmt.Errorf("creating deflate reader: %w", err)
			}
			bodyReader = zlibReader
		case "zstd":
			zstdReader, err := zstd.NewReader(bodyReader)
			if err != nil {
				return nil, fmt.Errorf("creating zstd reader: %w", err)
			}
			bodyReader = zstdReader.IOReadCloser()
		}
	}

	resp.Body = &bodyCompletionReader{inner: io.NopCloser(bodyReader), done: bodyDone}
	return resp, nil
}

type bodyCompletionReader struct {
	inner io.ReadCloser
	done  chan struct{}
	once  sync.Once
}

func (r *bodyCompletionReader) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if err != nil {
		r.once.Do(func() { close(r.done) })
	}
	return n, err
}

func (r *bodyCompletionReader) Close() error {
	r.once.Do(func() { close(r.done) })
	return r.inner.Close()
}

func parseStatusLine(line string) (proto, status string, statusCode int, err error) {
	dash := strings.Index(line, " ")
	if dash < 0 {
		return "", "", 0, fmt.Errorf("malformed status line: %q", line)
	}
	proto = line[:dash]
	rest := line[dash+1:]

	space := strings.Index(rest, " ")
	if space < 0 {
		return "", "", 0, fmt.Errorf("malformed status line: %q", line)
	}
	statusCode, err = strconv.Atoi(rest[:space])
	if err != nil {
		return "", "", 0, fmt.Errorf("malformed status code in: %q", line)
	}
	status = rest[space+1:]

	return proto, status, statusCode, nil
}

func shouldClose(protoMinor int, header http.Header) bool {
	conn := header.Get("Connection")
	if strings.EqualFold(conn, "close") {
		return true
	}
	if protoMinor == 0 && !strings.EqualFold(conn, "keep-alive") {
		return true
	}
	return false
}

func isChunked(transferEncoding []string) bool {
	for _, enc := range transferEncoding {
		if strings.EqualFold(enc, "chunked") {
			return true
		}
	}
	return false
}

func (c *http11Client) writeWARCFromConnection(ctx context.Context, reqTemp, respTemp spooledtempfile.ReadWriteSeekCloser, scheme string, pc *pooledConn, keepAlive bool, resp *http.Response, bodyDone chan struct{}, pi *protocolInfo) {
	defer c.client.WaitGroup.Done()

	select {
	case <-bodyDone:
	case <-ctx.Done():
		pc.Close()
		return
	}

	var feedbackChan chan FeedbackEvent
	batchSent := false
	if ctx.Value(ContextKeyFeedback) != nil {
		var ok bool
		feedbackChan, ok = ctx.Value(ContextKeyFeedback).(chan FeedbackEvent)
		if !ok {
			panic("feedback channel is not of type chan FeedbackEvent")
		}
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
			if keepAlive {
				c.pool.put(pc)
			} else {
				pc.Close()
			}
			return
		}

		c.client.ErrChan <- &Error{
			Err:  readErr,
			Func: "writeWARCFromConnection",
		}

		for record := range recordChan {
			if closeErr := record.Content.Close(); closeErr != nil {
				c.client.ErrChan <- &Error{
					Err:  closeErr,
					Func: "writeWARCFromConnection",
				}
			}
		}

		pc.Close()
		return
	}

	for record := range recordChan {
		select {
		case <-ctx.Done():
			pc.Close()
			return
		default:
			recordIDs = append(recordIDs, uuid.NewString())
			batch.Records = append(batch.Records, record)
		}
	}

	if len(batch.Records) != 2 {
		c.client.ErrChan <- &Error{
			Err:  errors.New("warc: unspecified problem creating WARC records"),
			Func: "writeWARCFromConnection",
		}
		for _, record := range batch.Records {
			record.Content.Close()
		}
		pc.Close()
		return
	}

	if batch.Records[0].Header.Get("WARC-Type") != "response" {
		slicesReverse(batch.Records)
	}

	var warcTargetURI string
	select {
	case recv, ok := <-targetURIRespCh:
		if !ok {
			panic("writeWARCFromConnection: targetURIRespCh closed unexpectedly")
		}
		warcTargetURI = recv
	case <-ctx.Done():
		pc.Close()
		return
	}

	for i, r := range batch.Records {
		select {
		case <-ctx.Done():
			pc.Close()
			return
		default:
			switch addr := pc.conn.RemoteAddr().(type) {
			case *net.TCPAddr:
				r.Header.Set("WARC-IP-Address", addr.IP.String())
			}

			r.Header.Add("WARC-Protocol", pi.ProtocolWARCValue())
			if tlsVal := pi.TLSVersionWARCValue(); tlsVal != "" {
				r.Header.Add("WARC-Protocol", tlsVal)
			}
			if pi.CipherSuite != 0 {
				r.Header.Set("WARC-Cipher-Suite", tlsCipherSuiteName(pi.CipherSuite))
			}
			r.Header.Set("WARC-Record-ID", "<urn:uuid:"+recordIDs[i]+">")

			if i == len(recordIDs)-1 {
				r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[0]+">")
			} else {
				r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[1]+">")
			}

			r.Header.Set("WARC-Target-URI", warcTargetURI)

			if _, seekErr := r.Content.Seek(0, 0); seekErr != nil {
				c.client.ErrChan <- &Error{Err: seekErr, Func: "writeWARCFromConnection"}
				pc.Close()
				return
			}

			digest, err := GetDigest(r.Content, c.client.DigestAlgorithm)
			if err != nil {
				c.client.ErrChan <- &Error{Err: err, Func: "writeWARCFromConnection"}
				pc.Close()
				return
			}

			r.Header.Set("WARC-Block-Digest", digest)
			r.Header.Set("Content-Length", strconv.FormatInt(getContentLength(r.Content), 10))

			if c.client.dedupeOptions.LocalDedupe {
				if r.Header.Get("WARC-Type") == "response" && !slicesContains(emptyPayloadDigests, r.Header.Get("WARC-Payload-Digest")) {
					captureTime, timeConversionErr := time.Parse(time.RFC3339, batch.CaptureTime)
					if timeConversionErr != nil {
						c.client.ErrChan <- &Error{Err: timeConversionErr, Func: "writeWARCFromConnection.timeConversionErr"}
						pc.Close()
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
		pc.Close()
		return
	}

	if keepAlive {
		c.pool.put(pc)
	} else {
		pc.Close()
	}
}

func readRequestFromTempShared(ctx context.Context, scheme string, client *CustomHTTPClient, reqTemp spooledtempfile.ReadWriteSeekCloser, targetURITxCh chan string, recordChan chan *Record) error {
	defer close(targetURITxCh)

	requestRecord := NewRecord(client.TempDir)
	recordChan <- requestRecord

	requestRecord.Header.Set("WARC-Type", "request")
	requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

	if _, err := reqTemp.Seek(0, 0); err != nil {
		return fmt.Errorf("readRequestFromTemp: seek failed: %w", err)
	}
	if _, err := io.Copy(requestRecord.Content, reqTemp); err != nil {
		return fmt.Errorf("readRequestFromTemp: copy failed: %w", err)
	}
	reqTemp.Close()

	warcTargetURI, err := parseRequestTargetURI(scheme, requestRecord.Content)
	if err != nil {
		return fmt.Errorf("readRequestFromTemp: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case targetURITxCh <- warcTargetURI:
	}

	return nil
}

func readResponseFromTempShared(ctx context.Context, client *CustomHTTPClient, respTemp spooledtempfile.ReadWriteSeekCloser, targetURIRxCh chan string, targetURITxCh chan string, recordChan chan *Record) error {
	defer close(targetURITxCh)

	responseRecord := NewRecord(client.TempDir)
	recordChan <- responseRecord

	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	if _, err := respTemp.Seek(0, 0); err != nil {
		return fmt.Errorf("readResponseFromTemp: seek failed: %w", err)
	}

	bytesCopied, err := io.Copy(responseRecord.Content, respTemp)
	if err != nil {
		return fmt.Errorf("readResponseFromTemp: copy failed: %w", err)
	}
	respTemp.Close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if _, err := responseRecord.Content.Seek(0, 0); err != nil {
		return fmt.Errorf("readResponseFromTemp: seek for ReadResponse failed: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(responseRecord.Content), nil)
	if err != nil {
		return fmt.Errorf("readResponseFromTemp: ReadResponse failed: %w", err)
	}

	var warcTargetURI, ok = <-targetURIRxCh
	if !ok {
		return errors.New("readResponseFromTemp: WARC-Target-URI channel closed due to readRequest error")
	}

	targetURITxCh <- warcTargetURI

	if v := ctx.Value(ContextKeySave); v != nil {
		saveCh := v.(chan bool)
		var save bool
		select {
		case save = <-saveCh:
		case <-ctx.Done():
			responseRecord.Content.Close()
			return ctx.Err()
		}
		if !save {
			resp.Body.Close()
			responseRecord.Content.Close()
			return errDiscarded
		}
	}

	payloadDigest, err := GetDigest(resp.Body, client.DigestAlgorithm)
	if err != nil {
		return fmt.Errorf("readResponseFromTemp: payload digest failed: %w", err)
	}

	resp.Body.Close()
	responseRecord.Header.Set("WARC-Payload-Digest", payloadDigest)

	var revisit = revisitRecord{}
	if bytesCopied >= int64(client.dedupeOptions.SizeThreshold) && !slicesContains(emptyPayloadDigests, payloadDigest) {
		if client.dedupeOptions.LocalDedupe {
			revisit, _ = client.dedupeHashTable.Get(payloadDigest)
			if revisit.targetURI != "" {
				LocalDedupeTotalBytes.Add(int64(revisit.size))
				LocalDedupeTotal.Add(1)
			}
		}

		if client.dedupeOptions.DoppelgangerDedupe && client.DigestAlgorithm == SHA1 && revisit.targetURI == "" {
			revisit, _ = checkDoppelgangerRevisit(client.dedupeOptions.DoppelgangerHost, payloadDigest, warcTargetURI)
			if revisit.targetURI != "" {
				DoppelgangerDedupeTotalBytes.Add(bytesCopied)
				DoppelgangerDedupeTotal.Add(1)
			}
		}

		if client.dedupeOptions.CDXDedupe && client.DigestAlgorithm == SHA1 && revisit.targetURI == "" {
			revisit, _ = checkCDXRevisit(client.dedupeOptions.CDXURL, payloadDigest, warcTargetURI, client.dedupeOptions.CDXCookie)
			if revisit.targetURI != "" {
				CDXDedupeTotalBytes.Add(bytesCopied)
				CDXDedupeTotal.Add(1)
			}
		}
	}

	if revisit.targetURI != "" && !slicesContains(emptyPayloadDigests, payloadDigest) {
		responseRecord.Header.Set("WARC-Type", "revisit")
		responseRecord.Header.Set("WARC-Refers-To-Target-URI", revisit.targetURI)
		responseRecord.Header.Set("WARC-Refers-To-Date", revisit.date.Format(time.RFC3339Nano))

		if revisit.responseUUID != "" {
			responseRecord.Header.Set("WARC-Refers-To", "<urn:uuid:"+revisit.responseUUID+">")
		}

		responseRecord.Header.Set("WARC-Profile", "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest")
		responseRecord.Header.Set("WARC-Truncated", "length")

		endOfHeadersOffset, err := findEndOfHeadersOffset(responseRecord.Content)
		if err != nil {
			return fmt.Errorf("readResponseFromTemp: %w", err)
		}
		if endOfHeadersOffset == -1 {
			return errors.New("readResponseFromTemp: could not find end of headers")
		}

		tempBuffer, err := spooledtempfile.NewSpooledTempFile("warc", client.TempDir)
		if err != nil {
			return fmt.Errorf("readResponseFromTemp: creating temp file: %w", err)
		}
		block := make([]byte, 1)
		wrote := 0
		responseRecord.Content.Seek(0, 0)
		for {
			n, err := responseRecord.Content.Read(block)
			if n > 0 {
				_, err = tempBuffer.Write(block)
				if err != nil {
					return fmt.Errorf("readResponseFromTemp: write to temp: %w", err)
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("readResponseFromTemp: read from content: %w", err)
			}
			wrote++
			if wrote == endOfHeadersOffset {
				break
			}
		}

		if err := responseRecord.Content.Close(); err != nil {
			return fmt.Errorf("readResponseFromTemp: close old content: %w", err)
		}
		responseRecord.Content = tempBuffer
	}

	return nil
}

func slicesReverse[S ~[]E, E any](s S) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func slicesContains[S ~[]E, E comparable](s S, v E) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}

func (c *http11Client) checkLocalRevisit(digest string) revisitRecord {
	revisit, exists := c.client.dedupeHashTable.Get(digest)
	if exists {
		return revisit
	}
	return revisitRecord{}
}
