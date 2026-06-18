package warc

import (
	"bufio"
	"context"
	"net"
	"sync"
	"time"

	tls "github.com/bogdanfinn/utls"
)

type pooledConn struct {
	conn    net.Conn
	br      *bufio.Reader
	host    string
	scheme  string
	created time.Time
}

func (pc *pooledConn) Close() error {
	if pc.conn != nil {
		return pc.conn.Close()
	}
	return nil
}

type connPool struct {
	mu          sync.Mutex
	conns       map[string][]*pooledConn
	maxIdle     int
	idleTimeout time.Duration
	dialer      *customDialer
	totalDials  int64
	activeConns int64
}

type ConnPoolStats struct {
	TotalHosts  int
	IdleConns   int
	MaxIdle     int
	IdleTimeout time.Duration
	TotalDials  int64
	ActiveConns int64
}

func (p *connPool) Stats() ConnPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := ConnPoolStats{
		TotalHosts:  len(p.conns),
		MaxIdle:     p.maxIdle,
		IdleTimeout: p.idleTimeout,
		TotalDials:  p.totalDials,
		ActiveConns: p.activeConns,
	}
	for _, conns := range p.conns {
		stats.IdleConns += len(conns)
	}
	return stats
}

func newConnPool(dialer *customDialer, maxIdle int, idleTimeout time.Duration) *connPool {
	return &connPool{
		conns:       make(map[string][]*pooledConn),
		maxIdle:     maxIdle,
		idleTimeout: idleTimeout,
		dialer:      dialer,
	}
}

func (p *connPool) key(host, scheme string) string {
	return scheme + "://" + host
}

func (p *connPool) get(host, scheme string) *pooledConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := p.key(host, scheme)
	conns := p.conns[key]
	for i := len(conns) - 1; i >= 0; i-- {
		pc := conns[i]
		if p.idleTimeout > 0 && time.Since(pc.created) > p.idleTimeout {
			pc.Close()
			conns = append(conns[:i], conns[i+1:]...)
			continue
		}
		if isConnAlive(pc.conn) {
			p.conns[key] = append(conns[:i], conns[i+1:]...)
			return pc
		}
		pc.Close()
		conns = append(conns[:i], conns[i+1:]...)
	}
	p.conns[key] = conns
	return nil
}

func (p *connPool) put(pc *pooledConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := p.key(pc.host, pc.scheme)
	conns := p.conns[key]
	if len(conns) >= p.maxIdle {
		pc.Close()
		p.activeConns--
		return
	}
	pc.created = time.Now()
	p.conns[key] = append(conns, pc)
	p.activeConns--
}

func (p *connPool) getOrCreate(ctx context.Context, host, scheme string) (*pooledConn, error) {
	if pc := p.get(host, scheme); pc != nil {
		return pc, nil
	}

	return p.dialNew(ctx, host, scheme)
}

func (p *connPool) dialNew(ctx context.Context, host, scheme string) (*pooledConn, error) {
	var conn net.Conn
	var err error

	if scheme == "https" {
		conn, err = p.dialer.dialTLSNew(ctx, "tcp", host)
	} else {
		conn, err = p.dialer.dialNew(ctx, "tcp", host)
	}
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.totalDials++
	p.activeConns++
	p.mu.Unlock()

	return &pooledConn{
		conn:   conn,
		br:     bufio.NewReaderSize(conn, 4096),
		host:   host,
		scheme: scheme,
	}, nil
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, conns := range p.conns {
		for _, pc := range conns {
			pc.Close()
		}
		delete(p.conns, key)
	}
}

func isConnAlive(conn net.Conn) bool {
	if conn == nil {
		return false
	}
	one := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	_, err := conn.Read(one)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return false
	}
	return true
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

func (d *customDialer) dialTLSNew(ctx context.Context, network, address string) (net.Conn, error) {
	plainConn, err := d.dialNew(ctx, network, address)
	if err != nil {
		return nil, err
	}

	serverName := address
	if host, _, err := net.SplitHostPort(address); err != nil {
		return nil, err
	} else {
		serverName = host
	}

	cfg := &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: d.client.insecureSkipVerifyCerts,
	}

	tlsConn := tls.UClient(plainConn, cfg, d.tlsProfile.clientHelloID,
		d.tlsProfile.withRandomTLSExtensionOrder, true, true)

	handshakeCtx, cancel := context.WithTimeout(ctx, d.client.TLSHandshakeTimeout)
	defer cancel()

	if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
		plainConn.Close()
		return nil, err
	}

	return tlsConn, nil
}
