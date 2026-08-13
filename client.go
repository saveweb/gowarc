package warc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
	DecompressBody          bool
	FollowRedirects         bool
	InsecureSkipVerifyCerts bool
	RandomLocalIP           bool
	DisableIPv4             bool
	DisableIPv6             bool
	IPv6AnyIP               bool
	DigestAlgorithm         DigestAlgorithm
	DisableKeepAlives       bool
	DefaultUserAgent        string
	ClientProfile           profiles.ClientProfile
	RandomTLSExtensionOrder bool
	MaxIdleConns            int
	MaxIdleConnsPerHost     int
	IdleConnTimeout         time.Duration
	EarlyCloseDrainLimit    int64
	EarlyCloseDrainTimeout  time.Duration
	MaxConcurrentDrains     int
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
	writerResults            []<-chan writerResult
	dedupeOptions            DedupeOptions
	TLSHandshakeTimeout      time.Duration
	ResponseHeaderTimeout    time.Duration
	ConnReadDeadline         time.Duration
	insecureSkipVerifyCerts  bool
	DigestAlgorithm          DigestAlgorithm
	recordIDVersion          UUIDVersion
	closeDNSCache            func()
	closeDedupeCache         func()
	randomLocalIP            bool
	DataTotal                *atomic.Int64
	disableKeepAlives        bool
	keepAliveMaxIdle         int
	keepAliveMaxIdlePerHost  int
	keepAliveIdleTimeout     time.Duration
	earlyCloseDrainLimit     int64
	earlyCloseDrainTimeout   time.Duration
	drainSlots               chan struct{}
	DecompressBody           bool
	defaultUserAgent         string
	followRedirects          bool
	randomTLSExtensionOrder  bool
	compatWG                 sync.WaitGroup
	lifecycleMu              sync.Mutex
	closing                  bool
	shutdownOnce             sync.Once
	shutdownDone             chan struct{}
	shutdownResult           FinalizeResult

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

type FinalizeResult struct {
	FinalizedFiles []string
	Err            error
}

// Shutdown stops new work and waits for every active capture and WARC writer
// to finish. The returned filenames have been flushed, closed and renamed.
// ctx controls only how long the caller waits; shutdown itself continues.
func (c *CustomHTTPClient) Shutdown(ctx context.Context) (FinalizeResult, error) {
	c.shutdownOnce.Do(func() {
		c.lifecycleMu.Lock()
		c.closing = true
		c.lifecycleMu.Unlock()
		go c.runShutdown()
	})
	select {
	case <-c.shutdownDone:
		result := c.shutdownResult
		result.FinalizedFiles = append([]string(nil), result.FinalizedFiles...)
		return result, result.Err
	case <-ctx.Done():
		return FinalizeResult{}, context.Cause(ctx)
	}
}

func (c *CustomHTTPClient) runShutdown() {
	defer close(c.shutdownDone)
	var wg sync.WaitGroup
	if c.protoClient != nil {
		c.protoClient.Shutdown()
	}
	c.WaitGroup.Wait()

	close(c.WARCWriter)

	wg.Add(len(c.warcWriterDoneChannels))
	for _, doneChan := range c.warcWriterDoneChannels {
		go func(done <-chan bool) {
			defer wg.Done()
			<-done
		}(doneChan)
	}

	wg.Wait()
	var writerErrs []error
	var finalizedFiles []string
	for _, resultChan := range c.writerResults {
		result := <-resultChan
		writerErrs = append(writerErrs, result.Err)
		finalizedFiles = append(finalizedFiles, result.FinalizedFiles...)
	}

	c.compatWG.Wait()
	close(c.ErrChan)

	if c.randomLocalIP {
		c.interfacesWatcherStop <- true
		close(c.interfacesWatcherStop)
	}

	c.closeDedupeCache()
	c.shutdownResult = FinalizeResult{FinalizedFiles: finalizedFiles, Err: errors.Join(writerErrs...)}
}

// Close is the compatibility form of Shutdown.
func (c *CustomHTTPClient) Close() error {
	_, err := c.Shutdown(context.Background())
	return err
}

func (c *CustomHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.lifecycleMu.Lock()
	if c.closing {
		c.lifecycleMu.Unlock()
		return nil, errors.New("warc: client is closing")
	}
	c.compatWG.Add(1)
	c.lifecycleMu.Unlock()
	exchange, err := c.Start(req)
	if exchange == nil {
		c.compatWG.Done()
		return nil, err
	}
	go func() {
		defer c.compatWG.Done()
		result, _ := exchange.Wait(context.Background())
		var archiveErrs []error
		for _, attempt := range result.Attempts {
			archiveErrs = append(archiveErrs, attempt.Err)
		}
		archiveErr := errors.Join(archiveErrs...)
		if archiveErr == nil {
			return
		}
		select {
		case c.ErrChan <- &Error{Err: archiveErr, Func: "Exchange.Wait"}:
		default:
		}
	}()
	return exchange.Response, err
}

// Start executes req and returns an Exchange whose Wait method reports the
// durable archival result independently from receiving response headers.
func (c *CustomHTTPClient) Start(req *http.Request) (*Exchange, error) {
	if req == nil {
		return nil, errors.New("warc: nil request")
	}
	c.lifecycleMu.Lock()
	if c.closing {
		c.lifecycleMu.Unlock()
		return nil, errors.New("warc: client is closing")
	}
	var feedback chan FeedbackEvent
	if value := req.Context().Value(ContextKeyFeedback); value != nil {
		var ok bool
		feedback, ok = value.(chan FeedbackEvent)
		if !ok {
			c.lifecycleMu.Unlock()
			return nil, errors.New("warc: feedback channel has invalid type")
		}
		if cap(feedback) == 0 {
			c.lifecycleMu.Unlock()
			return nil, errors.New("warc: feedback channel must be buffered")
		}
	}
	state := newExchangeState(c, feedback)
	ctx := context.WithValue(req.Context(), exchangeContextKey{}, state)
	req = req.Clone(ctx)
	req.URL.Scheme = strings.ToLower(req.URL.Scheme)
	c.WaitGroup.Add(1)
	c.lifecycleMu.Unlock()
	resp, err := c.protoClient.Do(ctx, req)
	c.WaitGroup.Done()
	exchange := &Exchange{Response: resp, state: state}
	return exchange, err
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
	reader, ok := body.(io.Reader)
	if body != nil && !ok {
		return nil, fmt.Errorf("warc: POST body has type %T, want io.Reader", body)
	}
	req, err := http.NewRequest(http.MethodPost, url, reader)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.Do(req)
}

func (c *CustomHTTPClient) CloseIdleConnections() {
	if c.protoClient != nil {
		c.protoClient.CloseIdleConnections()
	}
}

func NewWARCWritingHTTPClient(HTTPClientSettings HTTPClientSettings) (httpClient *CustomHTTPClient, err error) {
	if HTTPClientSettings.RotatorSettings == nil {
		return nil, errors.New("warc: RotatorSettings is required")
	}
	if err := checkRotatorSettings(HTTPClientSettings.RotatorSettings); err != nil {
		return nil, err
	}
	httpClient = new(CustomHTTPClient)
	httpClient.shutdownDone = make(chan struct{})

	httpClient.DataTotal = &DataTotal

	httpClient.CDXDedupeTotalBytes = &CDXDedupeTotalBytes
	httpClient.DoppelgangerDedupeTotalBytes = &DoppelgangerDedupeTotalBytes
	httpClient.LocalDedupeTotalBytes = &LocalDedupeTotalBytes

	httpClient.CDXDedupeTotal = &CDXDedupeTotal
	httpClient.DoppelgangerDedupeTotal = &DoppelgangerDedupeTotal
	httpClient.LocalDedupeTotal = &LocalDedupeTotal

	httpClient.randomLocalIP = HTTPClientSettings.RandomLocalIP

	httpClient.DigestAlgorithm = HTTPClientSettings.DigestAlgorithm
	httpClient.recordIDVersion = HTTPClientSettings.RotatorSettings.RecordIDVersion
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
	}
	constructed := false
	defer func() {
		if constructed {
			return
		}
		if httpClient.protoClient != nil {
			httpClient.protoClient.Shutdown()
		}
		httpClient.closeDedupeCache()
	}()

	if httpClient.dedupeOptions.SizeThreshold == 0 {
		httpClient.dedupeOptions.SizeThreshold = 2048
	}

	httpClient.ErrChan = make(chan *Error, 64)

	httpClient.insecureSkipVerifyCerts = HTTPClientSettings.InsecureSkipVerifyCerts

	if HTTPClientSettings.TempDir != "" {
		httpClient.TempDir = HTTPClientSettings.TempDir
		err = os.MkdirAll(httpClient.TempDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	httpClient.WaitGroup = new(WaitGroupWithCount)

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
	httpClient.ResponseHeaderTimeout = HTTPClientSettings.ResponseHeaderTimeout
	httpClient.ConnReadDeadline = HTTPClientSettings.ConnReadDeadline
	httpClient.DecompressBody = HTTPClientSettings.DecompressBody
	httpClient.disableKeepAlives = HTTPClientSettings.DisableKeepAlives
	httpClient.keepAliveMaxIdle = HTTPClientSettings.MaxIdleConns
	httpClient.keepAliveMaxIdlePerHost = HTTPClientSettings.MaxIdleConnsPerHost
	httpClient.keepAliveIdleTimeout = HTTPClientSettings.IdleConnTimeout
	if HTTPClientSettings.EarlyCloseDrainLimit == 0 {
		HTTPClientSettings.EarlyCloseDrainLimit = 1 << 20
	}
	if HTTPClientSettings.EarlyCloseDrainTimeout == 0 {
		HTTPClientSettings.EarlyCloseDrainTimeout = 2 * time.Second
	}
	if HTTPClientSettings.MaxConcurrentDrains == 0 {
		HTTPClientSettings.MaxConcurrentDrains = 32
	}
	httpClient.earlyCloseDrainLimit = HTTPClientSettings.EarlyCloseDrainLimit
	httpClient.earlyCloseDrainTimeout = HTTPClientSettings.EarlyCloseDrainTimeout
	if HTTPClientSettings.MaxConcurrentDrains > 0 {
		httpClient.drainSlots = make(chan struct{}, HTTPClientSettings.MaxConcurrentDrains)
	}

	httpClient.defaultUserAgent = HTTPClientSettings.DefaultUserAgent
	httpClient.followRedirects = HTTPClientSettings.FollowRedirects
	httpClient.randomTLSExtensionOrder = HTTPClientSettings.RandomTLSExtensionOrder

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
		h2c, err := newHTTP2Client(httpClient, false, false, false)
		if err != nil {
			return nil, err
		}
		httpClient.protoClient = h2c
	case "h3":
		h3c, err := newHTTP2Client(httpClient, false, true, false)
		if err != nil {
			return nil, err
		}
		httpClient.protoClient = h3c
	default:
		if HTTPClientSettings.EnableHTTP2 || HTTPClientSettings.EnableHTTP3 {
			h2c, err := newHTTP2Client(httpClient, HTTPClientSettings.EnableHTTP3, false, false)
			if err != nil {
				return nil, err
			}
			httpClient.protoClient = h2c
		} else {
			h1c, err := newHTTP2Client(httpClient, false, false, true)
			if err != nil {
				return nil, err
			}
			httpClient.protoClient = h1c
		}
	}

	httpClient.closeDNSCache = func() {
		if transport, ok := httpClient.protoClient.(*http2Client); ok && transport.dialer != nil {
			transport.dialer.close()
		}
	}

	httpClient.WARCWriter, httpClient.warcWriterDoneChannels, httpClient.writerResults, err = HTTPClientSettings.RotatorSettings.newWARCRotator()
	if err != nil {
		return nil, err
	}
	if httpClient.randomLocalIP {
		httpClient.interfacesWatcherStop = make(chan bool)
		httpClient.interfacesWatcherStarted = make(chan bool)
		go httpClient.getAvailableIPs(HTTPClientSettings.IPv6AnyIP)
		<-httpClient.interfacesWatcherStarted
	}
	constructed = true

	return httpClient, nil
}
