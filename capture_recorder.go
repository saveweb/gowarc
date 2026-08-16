package warc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	fhttp "github.com/saveweb/fhttp"
	"github.com/saveweb/gowarc/pkg/spooledtempfile"
)

type transportCaptureFactory struct {
	owner *http2Client
}

func (f *transportCaptureFactory) Begin(meta fhttp.CaptureAttemptMeta) (fhttp.CaptureAttempt, error) {
	// The transport, rather than the logical request, owns attempt boundaries:
	// retries must produce independent capture results and resource lifetimes.
	state := exchangeStateFromContext(meta.Request.Context())
	if state == nil {
		return nil, errors.New("warc: transport capture has no exchange state")
	}
	request, err := newTransportCaptureStream("warc-req", f.owner.client.TempDir, meta.Protocol, true)
	if err != nil {
		return nil, err
	}
	response, err := newTransportCaptureStream("warc-resp", f.owner.client.TempDir, meta.Protocol, false)
	if err != nil {
		_ = request.close()
		return nil, err
	}
	state.beginAttempt()
	return &transportCaptureAttempt{
		owner:    f.owner,
		state:    state,
		meta:     meta,
		request:  request,
		response: response,
	}, nil
}

type transportCaptureAttempt struct {
	owner    *http2Client
	state    *exchangeState
	meta     fhttp.CaptureAttemptMeta
	request  *transportCaptureStream
	response *transportCaptureStream
	once     sync.Once
}

func (a *transportCaptureAttempt) Request() fhttp.CaptureStream  { return a.request }
func (a *transportCaptureAttempt) Response() fhttp.CaptureStream { return a.response }

func (a *transportCaptureAttempt) Finish(result fhttp.CaptureAttemptResult) {
	a.once.Do(func() {
		attemptErr := result.Err
		if result.Outcome != fhttp.CaptureOutcomeComplete {
			// Transport teardown frequently reports a secondary socket error.
			// Preserve the request cancellation cause as the stable API signal.
			attemptErr = errors.Join(context.Cause(a.meta.Request.Context()), attemptErr)
		}
		// A capture-sink failure cannot produce a trustworthy WARC record.
		// Truncated network responses, however, are retained as partial records.
		if result.Outcome == fhttp.CaptureOutcomeFailed {
			cleanupErr := errors.Join(a.request.close(), a.response.close())
			a.state.finishAttempt(AttemptResult{Protocol: string(a.meta.Protocol), Outcome: result.Outcome, Err: attemptErr, cleanupErr: cleanupErr})
			return
		}

		requestFile, err := a.request.committedFile()
		if err != nil {
			cleanupErr := errors.Join(a.request.close(), a.response.close())
			a.state.finishAttempt(AttemptResult{Protocol: string(a.meta.Protocol), Outcome: result.Outcome, Err: err, cleanupErr: cleanupErr})
			return
		}
		responseFile, err := a.response.committedFile()
		if err != nil {
			cleanupErr := errors.Join(requestFile.Close(), a.response.close())
			a.state.finishAttempt(AttemptResult{Protocol: string(a.meta.Protocol), Outcome: result.Outcome, Err: err, cleanupErr: cleanupErr})
			return
		}

		pi := &protocolInfo{
			Protocol:    string(a.meta.Protocol),
			TLSVersion:  a.meta.TLSVersion,
			CipherSuite: a.meta.CipherSuite,
			QUICVersion: a.meta.QUICVersion,
			RemoteAddr:  a.meta.RemoteAddr,
		}
		if a.meta.Protocol == fhttp.CaptureHTTP1 {
			pi.Protocol = "http/1.1"
		}
		// Record construction and WARC I/O can outlive RoundTrip. Keep them off
		// the transport goroutine; Exchange.Commit owns durable completion.
		go func() {
			if a.state.waitForDecision() == exchangeDiscard {
				closeErr := errors.Join(requestFile.Close(), responseFile.Close())
				a.state.finishAttempt(AttemptResult{Protocol: pi.Protocol, Outcome: result.Outcome, cleanupErr: closeErr})
				return
			}
			events, writeErr := a.owner.writeCapturedExchange(a.meta.Request.Context(), a.meta.Request.URL.Scheme, a.meta.Request.Method, requestFile, responseFile, pi, result)
			a.state.finishAttempt(AttemptResult{Protocol: pi.Protocol, Outcome: result.Outcome, Records: events, Err: errors.Join(attemptErr, writeErr)})
		}()
	})
}

type transportCaptureStream struct {
	mu       sync.Mutex
	tempDir  string
	protocol fhttp.CaptureProtocol
	request  bool
	body     spooledtempfile.ReadWriteSeekCloser
	messages []fhttp.CaptureMessageMeta
	final    spooledtempfile.ReadWriteSeekCloser
	closed   bool
}

func newTransportCaptureStream(prefix, tempDir string, protocol fhttp.CaptureProtocol, request bool) (*transportCaptureStream, error) {
	body, err := spooledtempfile.NewSpooledTempFile(prefix+"-body", tempDir)
	if err != nil {
		return nil, err
	}
	return &transportCaptureStream{tempDir: tempDir, protocol: protocol, request: request, body: body}, nil
}

func (s *transportCaptureStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.final != nil {
		return 0, io.ErrClosedPipe
	}
	return s.body.Write(p)
}

func (s *transportCaptureStream) Message(meta fhttp.CaptureMessageMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.final != nil {
		return io.ErrClosedPipe
	}
	meta.HeaderFields = append([]fhttp.CaptureHeaderField(nil), meta.HeaderFields...)
	s.messages = append(s.messages, meta)
	return nil
}

func (s *transportCaptureStream) Commit(n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}
	if s.final != nil {
		return nil
	}
	if n < 0 || n > s.body.Len() {
		return fmt.Errorf("capture commit length %d exceeds %d", n, s.body.Len())
	}
	if err := s.body.Truncate(n); err != nil {
		return err
	}
	if s.protocol == fhttp.CaptureHTTP1 {
		// HTTP/1 is already application/http on the wire, so preserve the
		// transport's plaintext bytes exactly instead of serializing structs.
		s.final = s.body
		s.body = nil
		return nil
	}

	// HTTP/2 and HTTP/3 framing is not application/http. Build a stable
	// semantic representation from the ordered fields emitted by transport.
	final, err := spooledtempfile.NewSpooledTempFile("warc-message", s.tempDir)
	if err != nil {
		return err
	}
	if err := serializeSemanticMessage(final, s.protocol, s.request, s.messages, s.body); err != nil {
		_ = final.Close()
		return err
	}
	if err := s.body.Close(); err != nil {
		_ = final.Close()
		return err
	}
	s.body = nil
	s.final = final
	return nil
}

func (s *transportCaptureStream) committedFile() (spooledtempfile.ReadWriteSeekCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.final == nil {
		return nil, errors.New("capture stream was not committed")
	}
	return s.final, nil
}

func (s *transportCaptureStream) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if s.body != nil {
		errs = append(errs, s.body.Close())
	}
	if s.final != nil {
		errs = append(errs, s.final.Close())
	}
	return errors.Join(errs...)
}

func serializeSemanticMessage(dst io.Writer, protocol fhttp.CaptureProtocol, request bool, messages []fhttp.CaptureMessageMeta, body io.ReadSeeker) error {
	var initialMessages []fhttp.CaptureMessageMeta
	var trailers []fhttp.CaptureHeaderField
	for i := range messages {
		if messages[i].Trailers {
			trailers = append(trailers, messages[i].HeaderFields...)
		} else {
			initialMessages = append(initialMessages, messages[i])
		}
	}
	if len(initialMessages) == 0 {
		return errors.New("semantic capture has no initial headers")
	}
	if request && len(initialMessages) != 1 {
		return fmt.Errorf("semantic request capture has %d initial header blocks", len(initialMessages))
	}

	version := "HTTP/2.0"
	if protocol == fhttp.CaptureHTTP3 {
		version = "HTTP/3.0"
	}
	// Preserve informational responses in arrival order. Response trailers
	// belong only to the final response message.
	for i := range initialMessages {
		messageTrailers := []fhttp.CaptureHeaderField(nil)
		if i == len(initialMessages)-1 {
			messageTrailers = trailers
		}
		if err := serializeSemanticHeaders(dst, version, request, initialMessages[i], messageTrailers); err != nil {
			return err
		}
	}

	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if len(trailers) == 0 {
		_, err := io.Copy(dst, body)
		return err
	}
	// application/http needs chunk framing to delimit a body followed by
	// trailers; HTTP/2 and HTTP/3 DATA frames provide no reusable wire syntax.
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, writeErr := fmt.Fprintf(dst, "%x\r\n", n); writeErr != nil {
				return writeErr
			}
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if _, writeErr := io.WriteString(dst, "\r\n"); writeErr != nil {
				return writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	if _, err := io.WriteString(dst, "0\r\n"); err != nil {
		return err
	}
	for _, field := range trailers {
		if _, err := fmt.Fprintf(dst, "%s: %s\r\n", field.Name, field.Value); err != nil {
			return err
		}
	}
	_, err := io.WriteString(dst, "\r\n")
	return err
}

func serializeSemanticHeaders(dst io.Writer, version string, request bool, message fhttp.CaptureMessageMeta, trailers []fhttp.CaptureHeaderField) error {
	pseudo := make(map[string]string)
	for _, field := range message.HeaderFields {
		if strings.HasPrefix(field.Name, ":") {
			pseudo[field.Name] = field.Value
		}
	}
	if request {
		method := pseudo[":method"]
		if method == "" {
			method = http.MethodGet
		}
		target := pseudo[":path"]
		if target == "" {
			target = pseudo[":authority"]
		}
		if _, err := fmt.Fprintf(dst, "%s %s %s\r\n", method, target, version); err != nil {
			return err
		}
		if pseudo[":authority"] != "" {
			if _, err := fmt.Fprintf(dst, "Host: %s\r\n", pseudo[":authority"]); err != nil {
				return err
			}
		}
	} else {
		status := pseudo[":status"]
		if status == "" {
			return errors.New("semantic response capture has no :status")
		}
		statusCode, _ := strconv.Atoi(status)
		if _, err := fmt.Fprintf(dst, "%s %s %s\r\n", version, status, http.StatusText(statusCode)); err != nil {
			return err
		}
	}
	for _, field := range message.HeaderFields {
		// Pseudo-headers become the application/http start line. When trailers
		// exist, replace the protocol's length metadata with chunked framing so
		// standard HTTP/1 parsers can recover both body and trailers.
		if strings.HasPrefix(field.Name, ":") || strings.EqualFold(field.Name, "host") ||
			(len(trailers) > 0 && (strings.EqualFold(field.Name, "content-length") || strings.EqualFold(field.Name, "transfer-encoding") || strings.EqualFold(field.Name, "trailer"))) {
			continue
		}
		if _, err := fmt.Fprintf(dst, "%s: %s\r\n", field.Name, field.Value); err != nil {
			return err
		}
	}
	if len(trailers) > 0 {
		names := make([]string, 0, len(trailers))
		seen := make(map[string]bool)
		for _, field := range trailers {
			if key := strings.ToLower(field.Name); !seen[key] {
				seen[key] = true
				names = append(names, field.Name)
			}
		}
		if _, err := fmt.Fprintf(dst, "transfer-encoding: chunked\r\ntrailer: %s\r\n", strings.Join(names, ", ")); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(dst, "\r\n"); err != nil {
		return err
	}
	return nil
}
