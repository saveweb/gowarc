package warc

import (
	"compress/zlib"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	http "github.com/saveweb/fhttp"
)

type archiveResponseBody struct {
	transport io.ReadCloser
	reader    io.Reader
	client    *CustomHTTPClient
	decoder   io.Closer
	once      sync.Once
	mu        sync.Mutex
	eof       bool
	closeErr  error
}

func wrapArchiveResponseBody(client *CustomHTTPClient, resp *http.Response) error {
	transport := resp.Body
	if client.ConnReadDeadline > 0 {
		transport = &inactivityReadCloser{source: transport, timeout: client.ConnReadDeadline}
	}
	body := &archiveResponseBody{client: client}
	// Track EOF below decompression: only the transport framing boundary proves
	// that this connection is reusable and the captured response is complete.
	trackedTransport := &transportEOFReadCloser{
		ReadCloser: transport,
		onEOF: func() {
			body.mu.Lock()
			body.eof = true
			body.mu.Unlock()
		},
	}
	body.transport = trackedTransport
	body.reader = trackedTransport
	if client.DecompressBody {
		switch strings.ToLower(resp.Header.Get("Content-Encoding")) {
		case "gzip":
			reader, err := gzip.NewReader(body.transport)
			if err != nil {
				return err
			}
			body.reader = reader
			body.decoder = reader
		case "deflate":
			reader, err := zlib.NewReader(body.transport)
			if err != nil {
				return err
			}
			body.reader = reader
			body.decoder = reader
		case "zstd":
			reader, err := zstd.NewReader(body.transport)
			if err != nil {
				return err
			}
			body.decoder = reader.IOReadCloser()
			body.reader = body.decoder.(io.Reader)
		}
	}
	resp.Body = body
	return nil
}

type inactivityReadCloser struct {
	source  io.ReadCloser
	timeout time.Duration
	mu      sync.Mutex
	closed  bool
}

type transportEOFReadCloser struct {
	io.ReadCloser
	onEOF func()
}

func (r *transportEOFReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) {
		r.onEOF()
	}
	return n, err
}

func (r *inactivityReadCloser) Read(p []byte) (int, error) {
	// Some wrapped bodies don't expose SetReadDeadline. Closing the underlying
	// body is the only portable way to interrupt a stalled Read.
	timedOut := make(chan struct{})
	timer := time.AfterFunc(r.timeout, func() {
		close(timedOut)
		_ = r.source.Close()
	})
	n, err := r.source.Read(p)
	if !timer.Stop() {
		<-timedOut
		return n, os.ErrDeadlineExceeded
	}
	return n, err
}

func (r *inactivityReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.source.Close()
}

func (b *archiveResponseBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *archiveResponseBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		eof := b.eof
		b.mu.Unlock()
		if !eof && b.client.earlyCloseDrainLimit > 0 && b.client.drainSlots != nil {
			// Drain a bounded number of raw transport bytes to preserve keepalive.
			// The semaphore prevents many early Close calls from spawning an
			// unbounded number of blocked drain goroutines.
			select {
			case b.client.drainSlots <- struct{}{}:
				b.drain()
				<-b.client.drainSlots
			default:
			}
		}
		var decoderErr error
		if b.decoder != nil {
			decoderErr = b.decoder.Close()
		}
		b.closeErr = errors.Join(decoderErr, b.transport.Close())
	})
	return b.closeErr
}

func (b *archiveResponseBody) drain() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		remaining := b.client.earlyCloseDrainLimit
		buf := make([]byte, 32<<10)
		for remaining > 0 {
			next := int64(len(buf))
			if next > remaining {
				next = remaining
			}
			n, err := b.transport.Read(buf[:next])
			remaining -= int64(n)
			if errors.Is(err, io.EOF) {
				b.mu.Lock()
				b.eof = true
				b.mu.Unlock()
				return
			}
			if err != nil {
				return
			}
		}
	}()
	timer := time.NewTimer(b.client.earlyCloseDrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		_ = b.transport.Close()
		<-done
	}
}
