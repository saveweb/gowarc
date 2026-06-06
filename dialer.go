package warc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter"
	"github.com/miekg/dns"
)

type contextKey string

const (
	ContextKeyFeedback   contextKey = "feedback"
	ContextKeyWrappedConn contextKey = "wrappedConn"
	ContextKeySave       contextKey = "save"
)

func WithFeedbackChannel(ctx context.Context, feedbackChan chan struct{}) context.Context {
	return context.WithValue(ctx, ContextKeyFeedback, feedbackChan)
}

func WithWrappedConnection(ctx context.Context, wrappedConnChan chan *CustomConnection) context.Context {
	return context.WithValue(ctx, ContextKeyWrappedConn, wrappedConnChan)
}

func WithSaveChannel(ctx context.Context, ch chan bool) context.Context {
	return context.WithValue(ctx, ContextKeySave, ch)
}

var errDiscarded = errors.New("response discarded")

type dnsExchanger interface {
	ExchangeContext(ctx context.Context, m *dns.Msg, address string) (r *dns.Msg, rtt time.Duration, err error)
}

type customDialer struct {
	client             *CustomHTTPClient
	DNSConfig          *dns.ClientConfig
	DNSClient          dnsExchanger
	DNSRecords         *otter.Cache[string, dnsResult]
	tlsProfile         *TLSProfile
	net.Dialer
	disableIPv4        bool
	disableIPv6        bool
	dnsConcurrency     int
	dnsRoundRobinIndex atomic.Uint32
}

var emptyPayloadDigests = []string{
	"sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ",
	"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	"sha256:4OYMIQUY7QOBJGX36TEJS35ZEQT24QPEMSNZGTFESWMRW6CSXBKQ====",
	"blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
}

const happyEyeballsDelay = 300 * time.Millisecond

type dialResult struct {
	conn    net.Conn
	err     error
	primary bool
	done    bool
	ip      net.IP
}

func (d *customDialer) dialParallel(ctx context.Context, network string, primaryAddr, fallbackAddr string, primaryIP, fallbackIP net.IP) (net.Conn, net.IP, error) {
	if fallbackAddr == "" && primaryAddr == "" {
		return nil, nil, errors.New("no addresses available")
	}
	if fallbackAddr == "" {
		conn, err := d.dialSingle(ctx, network+"6", primaryAddr, primaryIP)
		return conn, primaryIP, err
	}
	if primaryAddr == "" {
		conn, err := d.dialSingle(ctx, network+"4", fallbackAddr, fallbackIP)
		return conn, fallbackIP, err
	}

	returned := make(chan struct{})
	defer close(returned)

	results := make(chan dialResult)

	startRacer := func(ctx context.Context, primary bool) {
		var addr string
		var ip net.IP
		var netType string
		if primary {
			addr, ip, netType = primaryAddr, primaryIP, network + "6"
		} else {
			addr, ip, netType = fallbackAddr, fallbackIP, network + "4"
		}
		conn, err := d.dialSingle(ctx, netType, addr, ip)
		select {
		case results <- dialResult{conn: conn, err: err, primary: primary, done: true, ip: ip}:
		case <-returned:
			if conn != nil {
				conn.Close()
			}
		}
	}

	var primary, fallback dialResult

	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go startRacer(primaryCtx, true)

	fallbackTimer := time.NewTimer(happyEyeballsDelay)
	defer fallbackTimer.Stop()

	for {
		select {
		case <-fallbackTimer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go startRacer(fallbackCtx, false)

		case res := <-results:
			if res.err == nil {
				return res.conn, res.ip, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, nil, primary.err
			}
			if res.primary && fallbackTimer.Stop() {
				fallbackTimer.Reset(0)
			}
		}
	}
}

func (d *customDialer) dialSingle(ctx context.Context, network, address string, resolvedIP net.IP) (net.Conn, error) {
	if d.client.randomLocalIP {
		localAddr := getLocalAddr(network, resolvedIP)
		if localAddr != nil {
			dialer := d.Dialer
			switch network {
			case "tcp", "tcp4", "tcp6":
				dialer.LocalAddr = localAddr.(*net.TCPAddr)
			case "udp", "udp4", "udp6":
				dialer.LocalAddr = localAddr.(*net.UDPAddr)
			}
			return dialer.DialContext(ctx, network, address)
		}
	}

	return d.DialContext(ctx, network, address)
}

func newCustomDialer(httpClient *CustomHTTPClient, DialTimeout, DNSRecordsTTL, DNSResolutionTimeout time.Duration, DNSCacheSize int, DNSServers []string, DNSFallback *dns.ClientConfig, DNSConcurrency int, disableIPv4, disableIPv6 bool) *customDialer {
	d := new(customDialer)

	d.Timeout = DialTimeout
	d.client = httpClient
	d.disableIPv4 = disableIPv4
	d.disableIPv6 = disableIPv6
	d.dnsConcurrency = DNSConcurrency

	DNScache, err := otter.MustBuilder[string, dnsResult](DNSCacheSize).
		WithTTL(DNSRecordsTTL).
		Build()
	if err != nil {
		panic(err)
	}

	d.DNSRecords = &DNScache

	d.DNSConfig, err = dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil || d.DNSConfig == nil {
		if DNSFallback != nil {
			d.DNSConfig = DNSFallback
		} else {
			panic(err)
		}
	}

	if len(DNSServers) > 0 {
		d.DNSConfig.Servers = DNSServers
	}

	d.DNSClient = &dns.Client{
		Net:     "udp",
		Timeout: DNSResolutionTimeout,
	}

	return d
}

// CustomConnection is kept for backward compatibility with WithWrappedConnection.
type CustomConnection struct {
	net.Conn
	io.Reader
	io.Writer
	closers []*io.PipeWriter
	sync.WaitGroup
	connReadDeadline time.Duration
	firstRead        sync.Once
}

func (cc *CustomConnection) setReadDeadline() error {
	if cc.connReadDeadline > 0 {
		if err := cc.Conn.SetReadDeadline(time.Now().Add(cc.connReadDeadline)); err != nil {
			return errors.New("CustomConnection.Read: SetReadDeadline failed: " + err.Error())
		}
	}
	return nil
}

func (cc *CustomConnection) Read(b []byte) (int, error) {
	cc.firstRead.Do(func() {
		if err := cc.setReadDeadline(); err != nil {
			cc.CloseWithError(err)
		}
	})
	c, err := cc.Reader.Read(b)
	if err != nil {
		cc.CloseWithError(err)
		return c, err
	}
	cc.setReadDeadline()
	return c, err
}

func (cc *CustomConnection) Write(b []byte) (int, error) {
	return cc.Writer.Write(b)
}

func (cc *CustomConnection) Close() error {
	return cc.CloseWithError(nil)
}

func (cc *CustomConnection) CloseWithError(err error) error {
	var closeErrors []error

	for _, c := range cc.closers {
		if closeErr := c.CloseWithError(err); closeErr != nil {
			closeErrors = append(closeErrors, fmt.Errorf("closing pipe writer failed: %w", closeErr))
		}
	}

	if connErr := cc.Conn.Close(); connErr != nil {
		closeErrors = append(closeErrors, fmt.Errorf("closing connection failed: %w", connErr))
	}

	return errors.Join(closeErrors...)
}
