package warc

import (
	"context"
	"crypto/tls"
	"net"

	http "git.saveweb.org/saveweb/fhttp"
)

type protocolInfo struct {
	Protocol     string
	TLSVersion   uint16
	CipherSuite  uint16
	RemoteAddr   net.Addr
	DNSAddresses []net.IP
}

func (pi *protocolInfo) ProtocolWARCValue() string {
	switch pi.Protocol {
	case "http/1.0":
		return "http/1.0"
	case "http/1.1":
		return "http/1.1"
	case "h2":
		return "h2"
	case "h3":
		return "h3"
	default:
		return pi.Protocol
	}
}

func (pi *protocolInfo) TLSVersionWARCValue() string {
	switch pi.TLSVersion {
	case tls.VersionTLS10:
		return "tls/1.0"
	case tls.VersionTLS11:
		return "tls/1.1"
	case tls.VersionTLS12:
		return "tls/1.2"
	case tls.VersionTLS13:
		return "tls/1.3"
	default:
		return ""
	}
}

func tlsCipherSuiteName(cs uint16) string {
	return tls.CipherSuiteName(cs)
}

type protocolClient interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, *protocolInfo, error)
	Close()
}
