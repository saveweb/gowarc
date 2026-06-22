package warc

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter"
	"github.com/miekg/dns"
	http "github.com/saveweb/fhttp"
	"github.com/saveweb/tls-client/profiles"
)

type Error struct {
	Err  error
	Func string
}

type HTTPClientSettings struct {
	RotatorSettings         *RotatorSettings
	TempDir                 string
	DNSServers              []string
	DNSFallback             *dns.ClientConfig
	DedupeOptions           DedupeOptions
	DialTimeout             time.Duration
	ResponseHeaderTimeout   time.Duration
	DNSResolutionTimeout    time.Duration
	DNSRecordsTTL           time.Duration
	DNSCacheSize            int
	DNSConcurrency          int
	TLSHandshakeTimeout     time.Duration
	ConnReadDeadline        time.Duration
	MaxReadBeforeTruncate   int // todo
	DecompressBody          bool
	FollowRedirects         bool
	InsecureSkipVerifyCerts bool
	RandomLocalIP           bool
	DisableIPv4             bool
	DisableIPv6             bool
	IPv6AnyIP               bool
	DigestAlgorithm         DigestAlgorithm
	EnableKeepAlive         bool
	DefaultUserAgent        string
	ClientProfile           profiles.ClientProfile
	RandomTLSExtensionOrder bool
	MaxIdleConns            int
	MaxIdleConnsPerHost     int
	IdleConnTimeout         time.Duration
	EnableHTTP2             bool
	EnableHTTP3             bool
	ForceProtocol           string // "http/1.1", "h2", "h3"
}

type CustomHTTPClient struct {
	interfacesWatcherStop    chan bool
	WaitGroup                *WaitGroupWithCount
	dedupeHashTable          *otter.Cache[string, revisitRecord]
	ErrChan                  chan *Error
	WARCWriter               chan *RecordBatch
	interfacesWatcherStarted chan bool
	protoClient              protocolClient
	TempDir                  string
	warcWriterDoneChannels   []chan bool
	dedupeOptions            DedupeOptions
	TLSHandshakeTimeout      time.Duration
	ConnReadDeadline         time.Duration
	MaxReadBeforeTruncate    int // todo
	insecureSkipVerifyCerts  bool
	DigestAlgorithm          DigestAlgorithm
	closeDNSCache            func()
	closeDedupeCache         func()
	randomLocalIP            bool
	DataTotal                *atomic.Int64
	enableKeepAlive          bool
	keepAliveMaxIdle         int
	keepAliveIdleTimeout     time.Duration
	DecompressBody           bool
	defaultUserAgent         string

	CDXDedupeTotalBytes          *atomic.Int64
	DoppelgangerDedupeTotalBytes *atomic.Int64
	LocalDedupeTotalBytes        *atomic.Int64

	CDXDedupeTotal          *atomic.Int64
	DoppelgangerDedupeTotal *atomic.Int64
	LocalDedupeTotal        *atomic.Int64

	dialTimeout          time.Duration
	dnsRecordsTTL        time.Duration
	dnsResolutionTimeout time.Duration
	dnsCacheSize         int
	dnsServers           []string
	dnsFallback          *dns.ClientConfig
	dnsConcurrency       int
	disableIPv4          bool
	disableIPv6          bool
	tlsProfile           *TLSProfile
}

func (c *CustomHTTPClient) Close() error {
	var wg sync.WaitGroup
	c.WaitGroup.Wait()

	if c.protoClient != nil {
		c.protoClient.Close()
	}

	close(c.WARCWriter)

	wg.Add(len(c.warcWriterDoneChannels))
	for _, doneChan := range c.warcWriterDoneChannels {
		go func(done chan bool) {
			defer wg.Done()
			<-done
		}(doneChan)
	}

	wg.Wait()

	close(c.ErrChan)

	if c.randomLocalIP {
		c.interfacesWatcherStop <- true
		close(c.interfacesWatcherStop)
	}

	c.closeDNSCache()
	c.closeDedupeCache()

	return nil
}

func (c *CustomHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, _, err := c.protoClient.Do(req.Context(), req)
	return resp, err
}

func (c *CustomHTTPClient) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func (c *CustomHTTPClient) Head(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func (c *CustomHTTPClient) Post(url, contentType string, body interface{}) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func (c *CustomHTTPClient) CloseIdleConnections() {
	if c.protoClient != nil {
		c.protoClient.Close()
	}
}

func (c *CustomHTTPClient) GetConnPoolStats() *ConnPoolStats {
	if h1, ok := c.protoClient.(*http11Client); ok {
		stats := h1.pool.Stats()
		return &stats
	}
	return nil
}

func NewWARCWritingHTTPClient(HTTPClientSettings HTTPClientSettings) (httpClient *CustomHTTPClient, err error) {
	httpClient = new(CustomHTTPClient)

	httpClient.DataTotal = &DataTotal

	httpClient.CDXDedupeTotalBytes = &CDXDedupeTotalBytes
	httpClient.DoppelgangerDedupeTotalBytes = &DoppelgangerDedupeTotalBytes
	httpClient.LocalDedupeTotalBytes = &LocalDedupeTotalBytes

	httpClient.CDXDedupeTotal = &CDXDedupeTotal
	httpClient.DoppelgangerDedupeTotal = &DoppelgangerDedupeTotal
	httpClient.LocalDedupeTotal = &LocalDedupeTotal

	httpClient.randomLocalIP = HTTPClientSettings.RandomLocalIP
	if httpClient.randomLocalIP {
		httpClient.interfacesWatcherStop = make(chan bool)
		httpClient.interfacesWatcherStarted = make(chan bool)
		go httpClient.getAvailableIPs(HTTPClientSettings.IPv6AnyIP)
		<-httpClient.interfacesWatcherStarted
	}

	httpClient.DigestAlgorithm = HTTPClientSettings.DigestAlgorithm
	HTTPClientSettings.RotatorSettings.digestAlgorithm = HTTPClientSettings.DigestAlgorithm

	httpClient.dedupeOptions = HTTPClientSettings.DedupeOptions

	dedupeCacheSize := HTTPClientSettings.DedupeOptions.DedupeCacheSize
	if dedupeCacheSize == 0 {
		dedupeCacheSize = 1_000_000
	}

	dedupeCache, err := otter.MustBuilder[string, revisitRecord](dedupeCacheSize).Build()
	if err != nil {
		return nil, err
	}
	httpClient.dedupeHashTable = &dedupeCache

	httpClient.closeDedupeCache = func() {
		httpClient.dedupeHashTable.Close()
		time.Sleep(1 * time.Second)
	}

	if httpClient.dedupeOptions.SizeThreshold == 0 {
		httpClient.dedupeOptions.SizeThreshold = 2048
	}

	httpClient.ErrChan = make(chan *Error)

	httpClient.insecureSkipVerifyCerts = HTTPClientSettings.InsecureSkipVerifyCerts

	if HTTPClientSettings.TempDir != "" {
		httpClient.TempDir = HTTPClientSettings.TempDir
		err = os.MkdirAll(httpClient.TempDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	if HTTPClientSettings.MaxReadBeforeTruncate == 0 {
		httpClient.MaxReadBeforeTruncate = 1000000000
	} else {
		httpClient.MaxReadBeforeTruncate = HTTPClientSettings.MaxReadBeforeTruncate
	}

	httpClient.WaitGroup = new(WaitGroupWithCount)

	httpClient.WARCWriter, httpClient.warcWriterDoneChannels, err = HTTPClientSettings.RotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, err
	}

	if HTTPClientSettings.DialTimeout == 0 {
		HTTPClientSettings.DialTimeout = 10 * time.Second
	}
	if HTTPClientSettings.ResponseHeaderTimeout == 0 {
		HTTPClientSettings.ResponseHeaderTimeout = 10 * time.Second
	}
	if HTTPClientSettings.TLSHandshakeTimeout == 0 {
		HTTPClientSettings.TLSHandshakeTimeout = 10 * time.Second
	}
	if HTTPClientSettings.DNSResolutionTimeout == 0 {
		HTTPClientSettings.DNSResolutionTimeout = 5 * time.Second
	}
	if HTTPClientSettings.DNSRecordsTTL == 0 {
		HTTPClientSettings.DNSRecordsTTL = 5 * time.Minute
	}
	if HTTPClientSettings.DNSCacheSize == 0 {
		HTTPClientSettings.DNSCacheSize = 10_000
	}

	httpClient.TLSHandshakeTimeout = HTTPClientSettings.TLSHandshakeTimeout
	httpClient.ConnReadDeadline = HTTPClientSettings.ConnReadDeadline
	httpClient.DecompressBody = HTTPClientSettings.DecompressBody
	httpClient.enableKeepAlive = HTTPClientSettings.EnableKeepAlive
	httpClient.keepAliveMaxIdle = HTTPClientSettings.MaxIdleConns
	httpClient.keepAliveIdleTimeout = HTTPClientSettings.IdleConnTimeout

	httpClient.defaultUserAgent = HTTPClientSettings.DefaultUserAgent

	httpClient.dialTimeout = HTTPClientSettings.DialTimeout
	httpClient.dnsRecordsTTL = HTTPClientSettings.DNSRecordsTTL
	httpClient.dnsResolutionTimeout = HTTPClientSettings.DNSResolutionTimeout
	httpClient.dnsCacheSize = HTTPClientSettings.DNSCacheSize
	httpClient.dnsServers = HTTPClientSettings.DNSServers
	httpClient.dnsFallback = HTTPClientSettings.DNSFallback
	httpClient.dnsConcurrency = HTTPClientSettings.DNSConcurrency
	httpClient.disableIPv4 = HTTPClientSettings.DisableIPv4
	httpClient.disableIPv6 = HTTPClientSettings.DisableIPv6

	httpClient.tlsProfile = NewTLSProfile(HTTPClientSettings.ClientProfile, HTTPClientSettings.RandomTLSExtensionOrder)

	switch HTTPClientSettings.ForceProtocol {
	case "h2":
		h2c, err := newHTTP2Client(httpClient, false, false)
		if err != nil {
			return nil, err
		}
		httpClient.protoClient = h2c
	case "h3":
		h3c, err := newHTTP2Client(httpClient, false, true)
		if err != nil {
			return nil, err
		}
		httpClient.protoClient = h3c
	default:
		if HTTPClientSettings.EnableHTTP2 || HTTPClientSettings.EnableHTTP3 {
			h2c, err := newHTTP2Client(httpClient, HTTPClientSettings.EnableHTTP3, false)
			if err != nil {
				return nil, err
			}
			httpClient.protoClient = h2c
		} else {
			httpClient.protoClient = newHTTP11Client(httpClient)
		}
	}

	httpClient.closeDNSCache = func() {
		if h1, ok := httpClient.protoClient.(*http11Client); ok {
			h1.pool.dialer.DNSRecords.Close()
		}
		time.Sleep(1 * time.Second)
	}

	return httpClient, nil
}
