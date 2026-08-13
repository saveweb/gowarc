package warc

import (
	"bytes"
	"crypto/tls"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestWARCProtocolValues(t *testing.T) {
	tests := []struct {
		name     string
		info     protocolInfo
		protocol string
		want     []string
	}{
		{name: "HTTP 1 cleartext", protocol: "http/1.1", want: []string{"http/1.1"}},
		{name: "HTTP 2 TLS", info: protocolInfo{TLSVersion: tls.VersionTLS12}, protocol: "h2", want: []string{"h2", "tls/1.2"}},
		{name: "HTTP 3 QUIC 1", info: protocolInfo{TLSVersion: tls.VersionTLS13, QUICVersion: quicVersion1}, protocol: "h3", want: []string{"h3", "quic/1", "tls/1.3"}},
		{name: "HTTP 3 QUIC 2", info: protocolInfo{TLSVersion: tls.VersionTLS13, QUICVersion: quicVersion2}, protocol: "h3", want: []string{"h3", "quic/2", "tls/1.3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, unknown := tt.info.WARCProtocolValues(tt.protocol)
			if !reflect.DeepEqual(got, tt.want) || len(unknown) != 0 {
				t.Fatalf("protocols = %v, unknown = %v, want %v and no unknown values", got, unknown, tt.want)
			}
		})
	}
}

func TestWARCProtocolValuesFiltersAndWarnsForUnknownLayers(t *testing.T) {
	info := protocolInfo{TLSVersion: 0x9999, QUICVersion: 0xdeadbeef}
	values, unknown := info.WARCProtocolValues("http/4")
	if len(values) != 0 {
		t.Fatalf("protocols = %v, want none", values)
	}
	if len(unknown) != 3 {
		t.Fatalf("unknown protocols = %v, want three layers", unknown)
	}
	want := []unknownProtocol{
		{Layer: "application", Value: "http/4"},
		{Layer: "transport", Value: "quic version 0xdeadbeef"},
		{Layer: "security", Value: "TLS version 0x9999"},
	}
	if !reflect.DeepEqual(unknown, want) {
		t.Fatalf("unknown protocols = %v, want %v", unknown, want)
	}
}

func TestProtocolWarningsDefaultToStderr(t *testing.T) {
	if protocolWarningLogger.Writer() != os.Stderr {
		t.Fatal("protocol warning logger does not write to stderr")
	}
}

func TestAddWARCProtocolsFiltersUnknownAndWarnsOnce(t *testing.T) {
	var output bytes.Buffer
	protocolWarningLogger.SetOutput(&output)
	protocolWarningLogger.SetFlags(0)
	t.Cleanup(func() {
		protocolWarningLogger.SetOutput(os.Stderr)
		protocolWarningLogger.SetFlags(logDefaultFlags)
	})

	info := &protocolInfo{TLSVersion: 0x9999, QUICVersion: 0xdeadbeef}
	warned := make(map[unknownProtocol]struct{})
	for range 2 {
		header := NewHeader()
		addWARCProtocols(header, info, "http/4", warned)
		if got := header.Values("WARC-Protocol"); len(got) != 0 {
			t.Fatalf("WARC-Protocol = %v, want none", got)
		}
	}
	got := output.String()
	for _, fragment := range []string{`layer=application value="http/4"`, `layer=transport value="quic version 0xdeadbeef"`, `layer=security value="TLS version 0x9999"`} {
		if count := strings.Count(got, fragment); count != 1 {
			t.Fatalf("warning %q appeared %d times in %q, want once", fragment, count, got)
		}
	}
}

func TestReaderPreservesRepeatedWARCProtocolFields(t *testing.T) {
	archive := "WARC/1.1\r\n" +
		"WARC-Type: response\r\n" +
		"WARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000001>\r\n" +
		"Content-Length: 0\r\n" +
		"WARC-Protocol: h3\r\n" +
		"WARC-Protocol: quic/2\r\n" +
		"WARC-Protocol: tls/1.3\r\n\r\n\r\n\r\n"
	reader, err := NewReader(strings.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatal(err)
	}
	defer record.Content.Close()
	if _, err := io.Copy(io.Discard, record.Content); err != nil {
		t.Fatal(err)
	}
	want := []string{"h3", "quic/2", "tls/1.3"}
	if got := record.Header.Values("WARC-Protocol"); !reflect.DeepEqual(got, want) {
		t.Fatalf("WARC-Protocol = %v, want %v", got, want)
	}
}
