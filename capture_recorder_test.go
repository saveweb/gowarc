package warc

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	http "github.com/saveweb/fhttp"
)

func TestSerializeSemanticRequest(t *testing.T) {
	body := strings.NewReader("payload")
	messages := []http.CaptureMessageMeta{{HeaderFields: []http.CaptureHeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":authority", Value: "example.test"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/upload?q=1"},
		{Name: "x-first", Value: "one"},
		{Name: "x-second", Value: "two"},
		{Name: "content-length", Value: "7"},
	}}}
	var serialized bytes.Buffer
	if err := serializeSemanticMessage(&serialized, http.CaptureHTTP2, true, messages, body); err != nil {
		t.Fatal(err)
	}
	want := "POST /upload?q=1 HTTP/2.0\r\nHost: example.test\r\nx-first: one\r\nx-second: two\r\ncontent-length: 7\r\n\r\npayload"
	if got := serialized.String(); got != want {
		t.Fatalf("serialized request = %q, want %q", got, want)
	}
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(serialized.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "payload" {
		t.Fatalf("parsed payload = %q", payload)
	}
}

func TestSerializeSemanticResponseWithTrailers(t *testing.T) {
	body := strings.NewReader("payload")
	messages := []http.CaptureMessageMeta{
		{HeaderFields: []http.CaptureHeaderField{
			{Name: ":status", Value: "200"},
			{Name: "content-length", Value: "7"},
			{Name: "x-response", Value: "yes"},
		}},
		{Trailers: true, HeaderFields: []http.CaptureHeaderField{
			{Name: "x-checksum", Value: "abc"},
		}},
	}
	var serialized bytes.Buffer
	if err := serializeSemanticMessage(&serialized, http.CaptureHTTP3, false, messages, body); err != nil {
		t.Fatal(err)
	}
	want := "HTTP/3.0 200 OK\r\nx-response: yes\r\ntransfer-encoding: chunked\r\ntrailer: x-checksum\r\n\r\n7\r\npayload\r\n0\r\nx-checksum: abc\r\n\r\n"
	if got := serialized.String(); got != want {
		t.Fatalf("serialized response = %q, want %q", got, want)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(serialized.Bytes())), nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "payload" {
		t.Fatalf("parsed payload = %q", payload)
	}
	if got := resp.Trailer.Get("x-checksum"); got != "abc" {
		t.Fatalf("parsed trailer = %q", got)
	}
}

func TestSerializeSemanticResponsePreservesInformationalMessages(t *testing.T) {
	messages := []http.CaptureMessageMeta{
		{HeaderFields: []http.CaptureHeaderField{
			{Name: ":status", Value: "103"},
			{Name: "link", Value: "</style.css>; rel=preload"},
		}},
		{HeaderFields: []http.CaptureHeaderField{
			{Name: ":status", Value: "200"},
			{Name: "content-length", Value: "2"},
		}},
	}
	var serialized bytes.Buffer
	if err := serializeSemanticMessage(&serialized, http.CaptureHTTP2, false, messages, strings.NewReader("ok")); err != nil {
		t.Fatal(err)
	}
	want := "HTTP/2.0 103 Early Hints\r\nlink: </style.css>; rel=preload\r\n\r\nHTTP/2.0 200 OK\r\ncontent-length: 2\r\n\r\nok"
	if got := serialized.String(); got != want {
		t.Fatalf("serialized response = %q, want %q", got, want)
	}
	reader := bufio.NewReader(bytes.NewReader(serialized.Bytes()))
	interim, err := http.ReadResponse(reader, nil)
	if err != nil || interim.StatusCode != http.StatusEarlyHints {
		t.Fatalf("interim=%v err=%v", interim, err)
	}
	final, err := http.ReadResponse(reader, nil)
	if err != nil || final.StatusCode != http.StatusOK {
		t.Fatalf("final=%v err=%v", final, err)
	}
	payload, err := io.ReadAll(final.Body)
	if err != nil || string(payload) != "ok" {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
}
