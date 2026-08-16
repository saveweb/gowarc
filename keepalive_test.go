package warc

import (
	"compress/gzip"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/saveweb/fhttp"
	"github.com/saveweb/fhttp/httptest"
)

func newConnectionCountingServer(handler http.Handler) (*httptest.Server, *atomic.Int64) {
	connections := &atomic.Int64{}
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	return server, connections
}

func TestDecompressedBodyStillCompletesTransportCapture(t *testing.T) {
	server, connections := newConnectionCountingServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, _ = io.WriteString(zw, "decoded payload")
		_ = zw.Close()
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: defaultRotatorSettings(t),
		DecompressBody:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
		exchange, err := client.Start(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(exchange.Response.Body)
		if err != nil || string(body) != "decoded payload" {
			t.Fatalf("body=%q err=%v", body, err)
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
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1", got)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func doCapturedRequest(t *testing.T, client *CustomHTTPClient, url string, closeEarly bool) {
	t.Helper()
	feedback := make(chan FeedbackEvent, 1)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req = req.WithContext(WithFeedbackChannel(req.Context(), feedback))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if !closeEarly {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		}
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-feedback:
	case <-time.After(2 * time.Second):
		t.Fatal("capture feedback timed out")
	}
}

func TestHTTP1KeepAliveDefaultsToEnabled(t *testing.T) {
	server, connections := newConnectionCountingServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "complete")
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: defaultRotatorSettings(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range client.ErrChan {
		}
	}()
	doCapturedRequest(t, client, server.URL+"/one", false)
	doCapturedRequest(t, client, server.URL+"/two", false)
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1 with default keepalive", got)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP1DisableKeepAlives(t *testing.T) {
	server, connections := newConnectionCountingServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "complete")
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:   defaultRotatorSettings(t),
		DisableKeepAlives: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range client.ErrChan {
		}
	}()
	doCapturedRequest(t, client, server.URL+"/one", false)
	doCapturedRequest(t, client, server.URL+"/two", false)
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want 2 with keepalive disabled", got)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP2KeepAliveAndDisableFlag(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disable     bool
		connections int64
	}{
		{name: "default reuse", connections: 1},
		{name: "disabled", disable: true, connections: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connections := &atomic.Int64{}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "complete")
			}))
			server.EnableHTTP2 = true
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			server.StartTLS()
			defer server.Close()

			client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
				RotatorSettings:         defaultRotatorSettings(t),
				ForceProtocol:           "h2",
				InsecureSkipVerifyCerts: true,
				DisableKeepAlives:       tc.disable,
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
				exchange, err := client.Start(req)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, exchange.Response.Body); err != nil {
					t.Fatal(err)
				}
				_ = exchange.Response.Body.Close()
				if _, err := exchange.Commit(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if got := connections.Load(); got != tc.connections {
				t.Fatalf("connections = %d, want %d", got, tc.connections)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTP1EarlyCloseDrainsAndReusesConnection(t *testing.T) {
	server, connections := newConnectionCountingServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "small response body")
	}))
	defer server.Close()

	client, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:        defaultRotatorSettings(t),
		EarlyCloseDrainLimit:   1024,
		EarlyCloseDrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range client.ErrChan {
		}
	}()
	doCapturedRequest(t, client, server.URL+"/early", true)
	doCapturedRequest(t, client, server.URL+"/next", false)
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1 after bounded early-close drain", got)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
