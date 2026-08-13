package warc

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"

	http "github.com/saveweb/fhttp"
)

type protocolInfo struct {
	Protocol    string
	TLSVersion  uint16
	CipherSuite uint16
	QUICVersion uint32
	RemoteAddr  net.Addr
}

type unknownProtocol struct {
	Layer string
	Value string
}

const logDefaultFlags = log.LstdFlags

var protocolWarningLogger = log.New(os.Stderr, "gowarc: ", logDefaultFlags)

var knownWARCProtocolIDs = map[string]struct{}{
	"dns": {}, "ftp": {}, "gemini": {}, "gopher": {},
	"http/0.9": {}, "http/1.0": {}, "http/1.1": {}, "h2": {}, "h2c": {}, "h3": {},
	"quic/1": {}, "quic/2": {},
	"spdy/1": {}, "spdy/2": {}, "spdy/3": {},
	"ssl/2": {}, "ssl/3": {},
	"tls/1.0": {}, "tls/1.1": {}, "tls/1.2": {}, "tls/1.3": {},
}

const (
	quicVersion1 uint32 = 0x00000001
	quicVersion2 uint32 = 0x6b3343cf
)

// WARCProtocolValues maps transport metadata to the identifiers registered by
// the IIPC WARC-Protocol proposal. Unknown non-empty layers are returned for
// warning and deliberately omitted from the WARC record.
func (pi *protocolInfo) WARCProtocolValues(applicationProtocol string) ([]string, []unknownProtocol) {
	var values []string
	var unknown []unknownProtocol
	add := func(layer, value string) {
		if value == "" {
			return
		}
		if _, ok := knownWARCProtocolIDs[value]; !ok {
			unknown = append(unknown, unknownProtocol{Layer: layer, Value: value})
			return
		}
		values = append(values, value)
	}

	add("application", applicationProtocol)
	switch pi.QUICVersion {
	case 0:
	case quicVersion1:
		add("transport", "quic/1")
	case quicVersion2:
		add("transport", "quic/2")
	default:
		unknown = append(unknown, unknownProtocol{Layer: "transport", Value: fmt.Sprintf("quic version %#x", pi.QUICVersion)})
	}
	switch pi.TLSVersion {
	case 0:
	case tls.VersionTLS10:
		add("security", "tls/1.0")
	case tls.VersionTLS11:
		add("security", "tls/1.1")
	case tls.VersionTLS12:
		add("security", "tls/1.2")
	case tls.VersionTLS13:
		add("security", "tls/1.3")
	default:
		unknown = append(unknown, unknownProtocol{Layer: "security", Value: fmt.Sprintf("TLS version %#x", pi.TLSVersion)})
	}
	return values, unknown
}

func warnUnknownProtocol(value unknownProtocol) {
	protocolWarningLogger.Printf("WARNING: omitting unknown WARC-Protocol layer=%s value=%q", value.Layer, value.Value)
}

func addWARCProtocols(header Header, pi *protocolInfo, applicationProtocol string, warned map[unknownProtocol]struct{}) {
	protocols, unknownProtocols := pi.WARCProtocolValues(applicationProtocol)
	for _, value := range protocols {
		header.Add("WARC-Protocol", value)
	}
	for _, value := range unknownProtocols {
		if _, alreadyWarned := warned[value]; alreadyWarned {
			continue
		}
		warned[value] = struct{}{}
		warnUnknownProtocol(value)
	}
}

func tlsCipherSuiteName(cs uint16) string {
	return tls.CipherSuiteName(cs)
}

type protocolClient interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
	CloseIdleConnections()
	Shutdown()
}
