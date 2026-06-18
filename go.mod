module git.saveweb.org/saveweb/gowarc

go 1.26.2

require (
	git.saveweb.org/saveweb/fhttp v0.6.8
	git.saveweb.org/saveweb/tls-client v1.14.0
	github.com/bogdanfinn/utls v1.7.7-barnius
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.6
	github.com/maypok86/otter v1.2.4
	github.com/miekg/dns v1.1.72
	github.com/remeh/sizedwaitgroup v1.0.0
	github.com/spf13/cobra v1.10.2
	github.com/ulikunitz/xz v0.5.15
	github.com/zeebo/blake3 v0.2.4
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.20.0
)

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/bdandy/go-errors v1.2.2 // indirect
	github.com/bdandy/go-socks4 v1.2.3 // indirect
	github.com/bogdanfinn/quic-go-utls v1.0.9-utls // indirect
	github.com/bogdanfinn/websocket v1.5.5-barnius // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/gammazero/deque v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/tam7t/hpkp v0.0.0-20160821193359-2b70b4024ed5 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/tools v0.44.0 // indirect
)

// Unsure exactly where these versions came from, but no longer exist. If we plan to publish under these versions, we need to remove them from this retract list.
retract (
	v1.1.2
	v1.1.0
	v1.0.0
)
