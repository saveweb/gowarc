package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	http "github.com/saveweb/fhttp"
	warc "github.com/saveweb/gowarc"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: testfetch <url> [h1|h2|h3]\n")
		fmt.Fprintf(os.Stderr, "  h1  - force HTTP/1.1 (default)\n")
		fmt.Fprintf(os.Stderr, "  h2  - enable HTTP/2\n")
		fmt.Fprintf(os.Stderr, "  h3  - enable HTTP/3\n")
		os.Exit(1)
	}

	targetURL := os.Args[1]
	proto := "h1"
	if len(os.Args) > 2 {
		proto = os.Args[2]
	}

	outDir := "warc_output"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}
	fmt.Printf("Output dir: %s\n", outDir)

	rotatorSettings := warc.NewRotatorSettings()
	rotatorSettings.Prefix = "TESTFETCH"
	rotatorSettings.OutputDirectory = outDir
	rotatorSettings.Compression = warc.CompressionGzip
	rotatorSettings.WARCSize = 1000

	settings := warc.HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DNSCacheSize:    100,
		DNSRecordsTTL:   5 * time.Minute,
		DecompressBody:  true,
		EnableKeepAlive: true,
		MaxIdleConns:    10,
		IdleConnTimeout: 90 * time.Second,
		DigestAlgorithm: warc.SHA256Base16,
	}

	switch proto {
	case "h2":
		settings.ForceProtocol = "h2"
	case "h3":
		settings.ForceProtocol = "h3"
	case "h1":
	default:
		log.Fatalf("unknown protocol %q, use h1, h2, or h3", proto)
	}

	client, err := warc.NewWARCWritingHTTPClient(settings)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	errChan := make(chan struct{})
	go func() {
		for range client.ErrChan {
			log.Printf("WARC error")
		}
		close(errChan)
	}()

	urls := []string{targetURL}
	if strings.Contains(targetURL, "example") {
		urls = append(urls, "https://http2.github.io/", "https://cloudflare.com/")
	}

	for _, u := range urls {
		fmt.Printf("Fetching %s (proto=%s)...\n", u, proto)

		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			log.Printf("  failed to create request: %v", err)
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			log.Printf("  failed: %v (%v)", err, elapsed)
			continue
		}

		body, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		fmt.Printf("  %s (%d bytes body, %v)\n", resp.Status, body, elapsed)
		fmt.Printf("  Proto: %s\n", resp.Proto)
		fmt.Printf("  Server: %s\n", resp.Header.Get("Server"))
		fmt.Printf("  Content-Type: %s\n", resp.Header.Get("Content-Type"))
	}

	stats := client.GetConnPoolStats()
	if stats != nil {
		fmt.Printf("\nConnection Pool Stats:\n")
		fmt.Printf("  Total Dials: %d\n", stats.TotalDials)
		fmt.Printf("  Active Conns: %d\n", stats.ActiveConns)
		fmt.Printf("  Idle Conns: %d\n", stats.IdleConns)
		fmt.Printf("  Total Hosts: %d\n", stats.TotalHosts)
		fmt.Printf("  Max Idle: %d\n", stats.MaxIdle)
	}

	if err := client.Close(); err != nil {
		log.Printf("close error: %v", err)
	}
	<-errChan

	entries, _ := os.ReadDir(outDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".warc.gz") {
			fmt.Printf("\nWARC file: %s\n", e.Name())
		}
	}

	fmt.Println("\nDone.")
}
