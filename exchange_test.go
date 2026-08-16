package warc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	http "github.com/saveweb/fhttp"
	"github.com/saveweb/fhttp/httptest"
)

func TestExchangeResultDoesNotRepeatWrappedAttemptError(t *testing.T) {
	attemptErr := errors.New("dial tcp4 192.0.2.1:443: i/o timeout")
	networkErr := fmt.Errorf("http2client: doing request: %w", attemptErr)
	state := &exchangeState{
		decision:   exchangeCommit,
		networkErr: networkErr,
		attempts:   []AttemptResult{{Err: attemptErr}},
	}

	result := state.result()
	if result.Err == nil {
		t.Fatal("result error is nil")
	}
	if got := strings.Count(result.Err.Error(), attemptErr.Error()); got != 1 {
		t.Fatalf("attempt error appears %d times in %q", got, result.Err)
	}
	if !errors.Is(result.Err, attemptErr) {
		t.Fatalf("result error does not wrap attempt error: %v", result.Err)
	}
}

func TestExchangeRecordIDVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	for _, test := range []struct {
		name    string
		version UUIDVersion
		want    uuid.Version
	}{
		{name: "default v7", want: uuid.Version(7)},
		{name: "configured v4", version: UUIDv4, want: uuid.Version(4)},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := defaultRotatorSettings(t)
			if test.version != "" {
				settings.RecordIDVersion = test.version
			}
			client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: settings})
			if err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			exchange, err := client.Start(req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, exchange.Response.Body); err != nil {
				t.Fatal(err)
			}
			if err := exchange.Response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := exchange.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}

			files, err := filepath.Glob(filepath.Join(settings.OutputDirectory, "*.warc.gz"))
			if err != nil || len(files) != 1 {
				t.Fatalf("WARC files = %v, err = %v", files, err)
			}
			file, err := os.Open(files[0])
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			reader, err := NewReader(file)
			if err != nil {
				t.Fatal(err)
			}
			var recordCount int
			for {
				record, err := reader.ReadRecord()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				recordCount++
				recordID := strings.TrimSuffix(strings.TrimPrefix(record.Header.Get("WARC-Record-ID"), "<urn:uuid:"), ">")
				id, err := uuid.Parse(recordID)
				if err != nil {
					t.Fatalf("parse WARC-Record-ID %q: %v", record.Header.Get("WARC-Record-ID"), err)
				}
				if got := id.Version(); got != test.want {
					t.Errorf("%s UUID version = %d, want %d", record.Header.Get("WARC-Type"), got, test.want)
				}
				_ = record.Content.Close()
			}
			if recordCount != 3 {
				t.Errorf("record count = %d, want 3", recordCount)
			}
		})
	}
}

func TestExchangeCommitPreservesHTTP1WireBytes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestWire := make(chan []byte, 1)
	responseWire := []byte("HTTP/1.1 200 Odd Reason\r\nx-MiXeD:  value\t\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nhello")
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		var request bytes.Buffer
		for {
			line, readErr := reader.ReadString('\n')
			request.WriteString(line)
			if readErr != nil || line == "\r\n" {
				break
			}
		}
		requestWire <- request.Bytes()
		_, _ = conn.Write(responseWire)
		<-time.After(100 * time.Millisecond)
	}()

	settings := defaultRotatorSettings(t)
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+"/wire?q=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Zed", "last")
	req.Header.Set("x-alpha", "first")
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(exchange.Response.Body); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := exchange.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Outcome != http.CaptureOutcomeComplete {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
	if len(result.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(result.Records))
	}
	actualRequestWire := <-requestWire
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	files, err := filepath.Glob(filepath.Join(settings.OutputDirectory, "*.warc.gz"))
	if err != nil || len(files) != 1 {
		t.Fatalf("WARC files = %v, err = %v", files, err)
	}
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	var archivedRequest, archivedResponse []byte
	var recordTypes []string
	recordIDs := make(map[string]string)
	concurrentTo := make(map[string]string)
	recordIPs := make(map[string]string)
	recordProtocols := make(map[string][]string)
	for {
		record, readErr := reader.ReadRecord()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		recordType := record.Header.Get("WARC-Type")
		recordTypes = append(recordTypes, recordType)
		if recordType == "request" || recordType == "response" {
			recordIDs[recordType] = record.Header.Get("WARC-Record-ID")
			concurrentTo[recordType] = record.Header.Get("WARC-Concurrent-To")
			recordIPs[recordType] = record.Header.Get("WARC-IP-Address")
			recordProtocols[recordType] = record.Header.Values("WARC-Protocol")
		}
		content, readErr := io.ReadAll(record.Content)
		_ = record.Content.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		switch record.Header.Get("WARC-Type") {
		case "request":
			archivedRequest = content
		case "response":
			archivedResponse = content
		}
	}
	if !bytes.Equal(archivedRequest, actualRequestWire) {
		t.Fatalf("archived request differs from wire\narchived: %q\nwire:     %q", archivedRequest, actualRequestWire)
	}
	if !bytes.Equal(archivedResponse, responseWire) {
		t.Fatalf("archived response differs from wire\narchived: %q\nwire:     %q", archivedResponse, responseWire)
	}
	if want := []string{"warcinfo", "request", "response"}; !slices.Equal(recordTypes, want) {
		t.Fatalf("WARC record order = %v, want %v", recordTypes, want)
	}
	if concurrentTo["request"] != recordIDs["response"] || concurrentTo["response"] != recordIDs["request"] {
		t.Fatalf("concurrent links = %v, record IDs = %v", concurrentTo, recordIDs)
	}
	wantIP := listener.Addr().(*net.TCPAddr).IP.String()
	if recordIPs["request"] != wantIP || recordIPs["response"] != wantIP {
		t.Fatalf("WARC-IP-Address = %v, want request and response to use peer IP %q", recordIPs, wantIP)
	}
	for recordType, protocols := range recordProtocols {
		if want := []string{"http/1.1"}; !slices.Equal(protocols, want) {
			t.Fatalf("%s WARC-Protocol = %v, want %v", recordType, protocols, want)
		}
	}
}

func TestExchangeCommitSupportsInternetArchiveRecordOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	settings := defaultRotatorSettings(t)
	settings.UseInternetArchiveRecordOrder = true
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, exchange.Response.Body); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := exchange.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	files, err := filepath.Glob(filepath.Join(settings.OutputDirectory, "*.warc.gz"))
	if err != nil || len(files) != 1 {
		t.Fatalf("WARC files = %v, err = %v", files, err)
	}
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	var recordTypes []string
	for {
		record, readErr := reader.ReadRecord()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		recordTypes = append(recordTypes, record.Header.Get("WARC-Type"))
		_ = record.Content.Close()
	}
	if want := []string{"warcinfo", "response", "request"}; !slices.Equal(recordTypes, want) {
		t.Fatalf("WARC record order = %v, want %v", recordTypes, want)
	}
}

func TestExchangeDiscardWritesNoHTTPRecords(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "discard me")
	}))
	defer server.Close()

	settings := defaultRotatorSettings(t)
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, exchange.Response.Body); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Discard(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Discard(context.Background()); err != nil {
		t.Fatalf("repeated Discard: %v", err)
	}
	if _, err := exchange.Commit(context.Background()); !errors.Is(err, ErrExchangeAlreadyDecided) {
		t.Fatalf("Commit after Discard error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	files, err := filepath.Glob(filepath.Join(settings.OutputDirectory, "*.warc.gz"))
	if err != nil || len(files) != 1 {
		t.Fatalf("WARC files = %v, err = %v", files, err)
	}
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatal(err)
	}
	_ = record.Content.Close()
	if got := record.Header.Get("WARC-Type"); got != "warcinfo" {
		t.Fatalf("first record type = %q", got)
	}
	if _, err := reader.ReadRecord(); err != io.EOF {
		t.Fatalf("record after warcinfo error = %v, want EOF", err)
	}
}

func TestExchangeDiscardClosesUnreadBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("x", 32))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:      defaultRotatorSettings(t),
		EarlyCloseDrainLimit: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exchange.Discard(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExchangeCommitCanResumeWaitingAfterTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: defaultRotatorSettings(t)})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := exchange.Commit(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Commit error = %v, want context.Canceled", err)
	}
	if _, err := io.Copy(io.Discard, exchange.Response.Body); err != nil {
		t.Fatal(err)
	}
	if err := exchange.Response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := exchange.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(result.Records))
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDiscardsUndecidedExchange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("x", 32))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:      defaultRotatorSettings(t),
		EarlyCloseDrainLimit: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := exchange.Commit(context.Background()); !errors.Is(err, ErrExchangeAlreadyDecided) {
		t.Fatalf("Commit after Shutdown error = %v", err)
	}
}

func TestExchangeCommitReturnsWriterFailure(t *testing.T) {
	server := newTestImageServer(t, http.StatusOK)
	defer server.Close()
	settings := NewRotatorSettings("writer-failure.test")
	settings.OutputDirectory = "/dev/full"
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/testdata/image.svg", nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, exchange.Response.Body)
	_ = exchange.Response.Body.Close()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := exchange.Commit(waitCtx)
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("Wait error = %v, result = %#v", err, result)
	}
	if closeErr := client.Close(); closeErr == nil {
		t.Fatal("Close returned nil after writer failure")
	}
}

func TestExchangeCommitReportsEarlyCloseAsTruncated(t *testing.T) {
	server, _ := newConnectionCountingServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer server.Close()
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:      defaultRotatorSettings(t),
		EarlyCloseDrainLimit: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = exchange.Response.Body.Close()
	result, err := exchange.Commit(context.Background())
	if err == nil {
		t.Fatal("Wait returned nil error for an abandoned response")
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Outcome != http.CaptureOutcomeTruncated {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExchangeCommitArchivesNetworkDisconnectAsTruncated(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil || line == "\r\n" {
				break
			}
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc")
	}()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: defaultRotatorSettings(t)})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String(), nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(exchange.Response.Body); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("body error = %v, want unexpected EOF", err)
	}
	_ = exchange.Response.Body.Close()
	result, err := exchange.Commit(context.Background())
	if err == nil {
		t.Fatal("Wait returned nil error after network disconnect")
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Outcome != http.CaptureOutcomeTruncated {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
	if len(result.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(result.Records))
	}
	for _, event := range result.Records {
		switch event.Header.Get("WARC-Type") {
		case "request":
			if got := event.Header.Get("WARC-Truncated"); got != "" {
				t.Fatalf("request WARC-Truncated = %q, want empty", got)
			}
		case "response":
			if got := event.Header.Get("WARC-Truncated"); got != "disconnect" {
				t.Fatalf("response WARC-Truncated = %q, want disconnect", got)
			}
			if got := event.Header.Get("WARC-Payload-Digest"); got != "" {
				t.Fatalf("partial payload digest = %q, want empty", got)
			}
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExchangeCommitArchivesContextCancellationAsTruncated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("x", 32))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: defaultRotatorSettings(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	if _, err := io.ReadFull(exchange.Response.Body, buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, _ = io.Copy(io.Discard, exchange.Response.Body)
	_ = exchange.Response.Body.Close()

	result, err := exchange.Commit(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want context.Canceled", err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Outcome != http.CaptureOutcomeTruncated {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
	if len(result.Records) != 2 {
		t.Fatalf("records = %d, want request and truncated response", len(result.Records))
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExchangeCommitHTTP2SemanticArchive(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Trailer", "X-Final")
		_, _ = w.Write(append([]byte("h2:"), body...))
		w.Header().Set("X-Final", "done")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	settings := defaultRotatorSettings(t)
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:         settings,
		ForceProtocol:           "h2",
		InsecureSkipVerifyCerts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/semantic", strings.NewReader("payload"))
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(exchange.Response.Body); err != nil || string(body) != "h2:payload" {
		t.Fatalf("body = %q, err = %v", body, err)
	}
	_ = exchange.Response.Body.Close()
	result, err := exchange.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Protocol != "h2" {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(settings.OutputDirectory, "*.warc.gz"))
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	for {
		record, readErr := reader.ReadRecord()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		if record.Header.Get("WARC-Type") == "request" {
			if got := record.Header.Values("WARC-Protocol"); !slices.Equal(got, []string{"h2", "tls/1.3"}) {
				t.Fatalf("request WARC-Protocol = %v", got)
			}
			req, err := http.ReadRequest(bufio.NewReader(record.Content))
			if err != nil || req.Proto != "HTTP/2.0" {
				t.Fatalf("semantic request proto=%v err=%v", req, err)
			}
		}
		if record.Header.Get("WARC-Type") == "response" {
			if got := record.Header.Values("WARC-Protocol"); !slices.Equal(got, []string{"h2", "tls/1.3"}) {
				t.Fatalf("response WARC-Protocol = %v", got)
			}
			resp, err := http.ReadResponse(bufio.NewReader(record.Content), nil)
			if err != nil || resp.Proto != "HTTP/2.0" || resp.Trailer.Get("X-Final") != "" {
				// Trailers are populated after consuming the chunked body.
				if err != nil || resp.Proto != "HTTP/2.0" {
					t.Fatalf("semantic response proto=%v err=%v", resp, err)
				}
			}
			_, err = io.Copy(io.Discard, resp.Body)
			if err != nil || resp.Trailer.Get("X-Final") != "done" {
				t.Fatalf("semantic response trailer=%v err=%v", resp.Trailer, err)
			}
		}
		_ = record.Content.Close()
	}
}

func TestExchangeCommitHTTP2EarlyCloseArchivesPartialResponse(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", 1024))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:         defaultRotatorSettings(t),
		ForceProtocol:           "h2",
		InsecureSkipVerifyCerts: true,
		EarlyCloseDrainLimit:    -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	exchange, err := client.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := exchange.Response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := exchange.Commit(context.Background())
	if err == nil {
		t.Fatal("Wait returned nil error for an abandoned HTTP/2 stream")
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Outcome != http.CaptureOutcomeTruncated {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
	if len(result.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(result.Records))
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPostSendsBodyAndContentType(t *testing.T) {
	type receivedRequest struct {
		contentType string
		body        string
	}
	received := make(chan receivedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- receivedRequest{contentType: r.Header.Get("Content-Type"), body: string(body)}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: defaultRotatorSettings(t)})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Post(server.URL, "application/json", strings.NewReader(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	got := <-received
	if got.contentType != "application/json" || got.body != `{"ok":true}` {
		t.Fatalf("received content-type=%q body=%q", got.contentType, got.body)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartRejectsUnbufferedFeedbackWithoutCorruptingLifecycle(t *testing.T) {
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: defaultRotatorSettings(t)})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	req = req.WithContext(WithFeedbackChannel(req.Context(), make(chan FeedbackEvent)))
	if _, err := client.Start(req); err == nil || !strings.Contains(err.Error(), "must be buffered") {
		t.Fatalf("Start error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownReturnsFinalizedFilesAndIsIdempotent(t *testing.T) {
	settings := defaultRotatorSettings(t)
	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.FinalizedFiles) != 1 || !slices.Equal(first.FinalizedFiles, second.FinalizedFiles) {
		t.Fatalf("first=%v second=%v", first.FinalizedFiles, second.FinalizedFiles)
	}
	finalized := filepath.Join(settings.OutputDirectory, first.FinalizedFiles[0])
	if _, err := os.Stat(finalized); err != nil {
		t.Fatalf("finalized WARC %q: %v", finalized, err)
	}
	if _, err := os.Stat(finalized + ".open"); !os.IsNotExist(err) {
		t.Fatalf("temporary WARC still exists: %v", err)
	}
}
