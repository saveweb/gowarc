package warc

import (
	"context"
	"crypto/tls"
	"net"

	http "github.com/saveweb/fhttp"
)

type protocolInfo struct {
	Protocol    string
	TLSVersion  uint16
	CipherSuite uint16
	QUICVersion uint32
	RemoteAddr  net.Addr
}

func tlsCipherSuiteName(cs uint16) string {
	return tls.CipherSuiteName(cs)
}

type protocolClient interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
	CloseIdleConnections()
	Shutdown()
}
