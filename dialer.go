package warc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter"
	"github.com/miekg/dns"
	"golang.org/x/sync/singleflight"
)

type contextKey string

const (
	ContextKeyFeedback contextKey = "feedback"
	ContextKeySave     contextKey = "save"
)

func WithFeedbackChannel(ctx context.Context, feedbackChan chan FeedbackEvent) context.Context {
	return context.WithValue(ctx, ContextKeyFeedback, feedbackChan)
}

func WithSaveChannel(ctx context.Context, ch chan bool) context.Context {
	return context.WithValue(ctx, ContextKeySave, ch)
}

var errDiscarded = errors.New("response discarded")

type dnsExchanger interface {
	ExchangeContext(ctx context.Context, m *dns.Msg, address string) (r *dns.Msg, rtt time.Duration, err error)
}

type customDialer struct {
	client     *CustomHTTPClient
	DNSConfig  *dns.ClientConfig
	DNSClient  dnsExchanger
	DNSRecords *otter.Cache[string, dnsResult]
	net.Dialer
	disableIPv4        bool
	disableIPv6        bool
	dnsConcurrency     int
	dnsRoundRobinIndex atomic.Uint32
	dnsGroup           singleflight.Group
	lookupCtx          context.Context
	lookupCancel       context.CancelFunc
	lookupMu           sync.Mutex
	lookupWG           sync.WaitGroup
	lookupClosing      bool
}

func (d *customDialer) beginLookup() bool {
	d.lookupMu.Lock()
	defer d.lookupMu.Unlock()
	if d.lookupClosing {
		return false
	}
	if d.lookupCtx == nil {
		d.lookupCtx, d.lookupCancel = context.WithCancel(context.WithValue(context.Background(), writerOwnerContextKey{}, true))
	}
	d.lookupWG.Add(1)
	return true
}

func (d *customDialer) close() {
	d.lookupMu.Lock()
	if !d.lookupClosing {
		d.lookupClosing = true
		if d.lookupCancel != nil {
			d.lookupCancel()
		}
	}
	d.lookupMu.Unlock()
	d.lookupWG.Wait()
	d.DNSRecords.Close()
	// otter v1.2.4 exposes no join primitive for its one-second TTL cleanup
	// loop. Keep resolver shutdown synchronous until the dependency does.
	time.Sleep(1100 * time.Millisecond)
}

func (d *customDialer) dialNew(ctx context.Context, network, address string) (net.Conn, error) {
	ipv4, ipv6, _, err := d.archiveDNS(ctx, address)
	if err != nil {
		return nil, err
	}

	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	var ipv4Addr, ipv6Addr string
	if ipv4 != nil {
		ipv4Addr = net.JoinHostPort(ipv4.String(), port)
	}
	if ipv6 != nil {
		ipv6Addr = net.JoinHostPort(ipv6.String(), port)
	}
	conn, _, err := d.dialParallel(ctx, network, ipv6Addr, ipv4Addr, ipv6, ipv4)
	return conn, err
}

func (d *customDialer) resolveUDPAddr(ctx context.Context, address string) (*net.UDPAddr, error) {
	ipv4, ipv6, _, err := d.archiveDNS(ctx, address)
	if err != nil {
		return nil, err
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ip := ipv6
	if ip == nil {
		ip = ipv4
	}
	return net.ResolveUDPAddr("udp", net.JoinHostPort(ip.String(), port))
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
	baseNetwork := strings.TrimSuffix(strings.TrimSuffix(network, "4"), "6")
	if fallbackAddr == "" && primaryAddr == "" {
		return nil, nil, errors.New("no addresses available")
	}
	if fallbackAddr == "" {
		conn, err := d.dialSingle(ctx, baseNetwork+"6", primaryAddr, primaryIP)
		return conn, primaryIP, err
	}
	if primaryAddr == "" {
		conn, err := d.dialSingle(ctx, baseNetwork+"4", fallbackAddr, fallbackIP)
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
			addr, ip, netType = primaryAddr, primaryIP, baseNetwork+"6"
		} else {
			addr, ip, netType = fallbackAddr, fallbackIP, baseNetwork+"4"
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

func newCustomDialer(httpClient *CustomHTTPClient, DialTimeout, DNSRecordsTTL, DNSResolutionTimeout time.Duration, DNSCacheSize int, DNSServers []string, DNSFallback *dns.ClientConfig, DNSConcurrency int, disableIPv4, disableIPv6 bool) (*customDialer, error) {
	d := new(customDialer)
	d.lookupCtx, d.lookupCancel = context.WithCancel(context.WithValue(context.Background(), writerOwnerContextKey{}, true))
	if DNSResolutionTimeout <= 0 {
		DNSResolutionTimeout = 5 * time.Second
	}

	d.Timeout = DialTimeout
	d.client = httpClient
	d.disableIPv4 = disableIPv4
	d.disableIPv6 = disableIPv6
	d.dnsConcurrency = DNSConcurrency

	var err error
	d.DNSConfig, err = dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil || d.DNSConfig == nil {
		if DNSFallback != nil {
			d.DNSConfig = DNSFallback
		} else {
			return nil, fmt.Errorf("read resolver configuration: %w", err)
		}
	}

	DNScache, err := otter.MustBuilder[string, dnsResult](DNSCacheSize).
		WithTTL(DNSRecordsTTL).
		Build()
	if err != nil {
		return nil, err
	}

	d.DNSRecords = &DNScache

	if len(DNSServers) > 0 {
		d.DNSConfig.Servers = DNSServers
	}

	d.DNSClient = &dns.Client{
		Net:     "udp",
		Timeout: DNSResolutionTimeout,
	}

	return d, nil
}
