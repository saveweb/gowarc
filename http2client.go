package warc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	http "github.com/saveweb/fhttp"
	"github.com/saveweb/gowarc/pkg/spooledtempfile"
	tls_client "github.com/saveweb/tls-client"
)

type http2Client struct {
	client    *CustomHTTPClient
	tlsClient tls_client.HttpClient
	dialer    *customDialer
}

func newHTTP2Client(client *CustomHTTPClient, enableH3 bool, forceH3 bool, forceH1 bool) (*http2Client, error) {
	c := &http2Client{client: client}
	factory := &transportCaptureFactory{owner: c}
	opts := []tls_client.HttpClientOption{
		tls_client.WithClientProfile(client.tlsProfile.clientProfile),
		tls_client.WithTimeoutMilliseconds(0),
	}
	if client.randomTLSExtensionOrder {
		opts = append(opts, tls_client.WithRandomTLSExtensionOrder())
	}
	if !client.followRedirects {
		opts = append(opts, tls_client.WithNotFollowRedirects())
	}

	if client.insecureSkipVerifyCerts {
		opts = append(opts, tls_client.WithInsecureSkipVerify())
	}

	if forceH3 {
		opts = append(opts, tls_client.WithForceH3())
	} else if forceH1 {
		opts = append(opts, tls_client.WithForceHttp1())
	} else if !enableH3 {
		opts = append(opts, tls_client.WithDisableHttp3())
	}

	to := &tls_client.TransportOptions{
		MaxIdleConns:          client.keepAliveMaxIdle,
		MaxIdleConnsPerHost:   client.keepAliveMaxIdlePerHost,
		DisableKeepAlives:     client.disableKeepAlives,
		DisableCompression:    true,
		CaptureFactory:        factory,
		ResponseHeaderTimeout: client.ResponseHeaderTimeout,
		TLSHandshakeTimeout:   client.TLSHandshakeTimeout,
	}
	if client.keepAliveIdleTimeout > 0 {
		idle := client.keepAliveIdleTimeout
		to.IdleConnTimeout = &idle
	}
	opts = append(opts, tls_client.WithTransportOptions(to))

	var err error
	c.dialer, err = newCustomDialer(client, client.dialTimeout, client.dnsRecordsTTL, client.dnsResolutionTimeout,
		client.dnsCacheSize, client.dnsServers, client.dnsFallback, client.dnsConcurrency,
		client.disableIPv4, client.disableIPv6)
	if err != nil {
		return nil, fmt.Errorf("http2client: creating dialer: %w", err)
	}
	to.ResolveUDPAddr = c.dialer.resolveUDPAddr
	if !forceH3 {
		opts = append(opts, tls_client.WithDialContext(c.dialer.dialNew))
	}

	if client.disableIPv4 {
		opts = append(opts, tls_client.WithDisableIPV4())
	}
	if client.disableIPv6 {
		opts = append(opts, tls_client.WithDisableIPV6())
	}

	tc, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
	if err != nil {
		c.dialer.close()
		return nil, fmt.Errorf("http2client: creating tls-client: %w", err)
	}

	c.tlsClient = tc
	return c, nil
}

func (c *http2Client) CloseIdleConnections() {
	c.tlsClient.CloseIdleConnections()
}

func (c *http2Client) Shutdown() {
	c.CloseIdleConnections()
	if c.dialer != nil {
		c.dialer.close()
	}
}

func (c *http2Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") == "" && c.client.defaultUserAgent != "" {
		req.Header.Set("User-Agent", c.client.defaultUserAgent)
	}
	resp, err := c.tlsClient.Do(req.WithContext(ctx))
	state := exchangeStateFromContext(ctx)
	if state == nil {
		return nil, errors.New("warc: request has no exchange state")
	}
	if err != nil {
		state.finishNetwork(err)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, fmt.Errorf("http2client: doing request: %w", err)
	}
	if err := wrapArchiveResponseBody(c.client, resp); err != nil {
		_ = resp.Body.Close()
		state.finishNetwork(err)
		return nil, fmt.Errorf("http2client: wrapping response body: %w", err)
	}
	state.finishNetwork(nil)

	return resp, nil
}

func (c *http2Client) writeCapturedExchange(ctx context.Context, scheme string, reqTemp, respTemp spooledtempfile.ReadWriteSeekCloser, pi *protocolInfo, captureResult http.CaptureAttemptResult) (FeedbackEvent, error) {
	requestRecord, target, err := buildRequestRecord(scheme, c.client, reqTemp)
	if err != nil {
		_ = respTemp.Close()
		return nil, err
	}
	responseRecord, err := buildResponseRecord(ctx, c.client, respTemp, target, captureResult.Outcome == http.CaptureOutcomeTruncated)
	if err != nil {
		_ = requestRecord.Content.Close()
		if errors.Is(err, errDiscarded) {
			return nil, nil
		}
		return nil, err
	}

	requestRecordID, err := newUUID(c.client.recordIDVersion)
	if err != nil {
		_ = responseRecord.Content.Close()
		_ = requestRecord.Content.Close()
		return nil, err
	}
	responseRecordID, err := newUUID(c.client.recordIDVersion)
	if err != nil {
		_ = responseRecord.Content.Close()
		_ = requestRecord.Content.Close()
		return nil, err
	}
	requestRecord.Header.Set("WARC-Record-ID", "<urn:uuid:"+requestRecordID+">")
	requestRecord.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+responseRecordID+">")
	responseRecord.Header.Set("WARC-Record-ID", "<urn:uuid:"+responseRecordID+">")
	responseRecord.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+requestRecordID+">")
	if captureResult.Outcome == http.CaptureOutcomeTruncated {
		responseRecord.Header.Set("WARC-Truncated", "disconnect")
	}

	batch := NewRecordBatch(nil)
	batch.Records = []*Record{requestRecord, responseRecord}
	responseSize := responseRecord.Content.Len()
	closeRecords := true
	defer func() {
		if closeRecords {
			_ = responseRecord.Content.Close()
			_ = requestRecord.Content.Close()
		}
	}()

	// IIPC defines the request IP as the destination and the response IP as the
	// source. For one direct exchange both are the same transport peer.
	warnedProtocols := make(map[unknownProtocol]struct{})
	for _, record := range batch.Records {
		if pi.RemoteAddr != nil {
			switch addr := pi.RemoteAddr.(type) {
			case *net.TCPAddr:
				record.Header.Set("WARC-IP-Address", addr.IP.String())
			case *net.UDPAddr:
				record.Header.Set("WARC-IP-Address", addr.IP.String())
			}
		}
		protocol := pi.Protocol
		if pi.Protocol == "http/1.1" {
			if detected := detectHTTPVersion(record.Content); detected != "" {
				protocol = detected
			}
		}
		addWARCProtocols(record.Header, pi, protocol, warnedProtocols)
		if pi.CipherSuite != 0 {
			record.Header.Set("WARC-Cipher-Suite", tlsCipherSuiteName(pi.CipherSuite))
		}
		record.Header.Set("WARC-Target-URI", target)
		if _, err := record.Content.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		digest, err := GetDigest(record.Content, c.client.DigestAlgorithm)
		if err != nil {
			return nil, err
		}
		size := record.Content.Len()
		if size < 0 {
			return nil, errors.New("warc: cannot stat captured record")
		}
		record.Header.Set("WARC-Block-Digest", digest)
		record.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	}

	// The request context owns network I/O, but cancellation must not discard
	// bytes that the transport has already committed to this capture attempt.
	// finishAttempt keeps Shutdown aware of this writer submission.
	c.client.WARCWriter <- batch
	closeRecords = false
	writeResult, err := batch.Wait(context.Background())
	if err != nil {
		return nil, err
	}
	if c.client.dedupeOptions.LocalDedupe && responseRecord.Header.Get("WARC-Type") == "response" &&
		!slicesContains(emptyPayloadDigests, responseRecord.Header.Get("WARC-Payload-Digest")) {
		captureTime, err := time.Parse(time.RFC3339Nano, batch.CaptureTime)
		if err != nil {
			return writeResult.Events, err
		}
		c.client.dedupeHashTable.Set(responseRecord.Header.Get("WARC-Payload-Digest"), revisitRecord{
			responseUUID: responseRecordID,
			size:         responseSize,
			targetURI:    target,
			date:         captureTime,
		})
	}
	return writeResult.Events, nil
}

func detectHTTPVersion(content spooledtempfile.ReadWriteSeekCloser) string {
	prefix := make([]byte, 64)
	n, _ := content.ReadAt(prefix, 0)
	line := string(prefix[:n])
	switch {
	case strings.HasPrefix(line, "HTTP/1.0 "), strings.Contains(line, " HTTP/1.0\r\n"):
		return "http/1.0"
	case strings.HasPrefix(line, "HTTP/1.1 "), strings.Contains(line, " HTTP/1.1\r\n"):
		return "http/1.1"
	default:
		return ""
	}
}

var _ = (protocolClient)((*http2Client)(nil))
