# warc

[![GoDoc](https://godoc.org/github.com/internetarchive/gowarc?status.svg)](https://godoc.org/github.com/internetarchive/gowarc)
[![Go Report Card](https://goreportcard.com/badge/github.com/internetarchive/gowarc)](https://goreportcard.com/report/github.com/internetarchive/gowarc)

A Go library for reading and writing [WARC files](https://iipc.github.io/warc-specifications/), with advanced features for web archiving.

## Features

- Read and write WARC files with support for multiple compression formats (GZIP, ZSTD)
- HTTP client with built-in WARC recording capabilities
- Content deduplication (local URL-agnostic and CDX-based)
- Configurable file rotation and size limits
- DNS caching and custom DNS resolution (with DNS archiving)
- Custom TLS configurations
- Random local IP assignment for distributed crawling (including Linux kernel AnyIP feature)
- Smart memory management with disk spooling options
- IPv4/IPv6 support with configurable preferences

## Installation

```bash
go get github.com/internetarchive/gowarc
```

## Usage

This library's biggest feature is to provide a standard HTTP client through which you can execute requests that will be recorded automatically to WARC files. It's the basis of [Zeno](https://github.com/internetarchive/Zeno).

### HTTP Client with WARC Recording

```go
package main

import (
	"context"
	"io"

	http "github.com/saveweb/fhttp"
	warc "github.com/saveweb/gowarc"
)

func main() {
	// Configure WARC settings
	rotator := warc.NewRotatorSettings("crawler.example.com")
	rotator.Prefix = "WEB"
	rotator.OutputDirectory = "./warcs"
	// WARC record IDs use UUIDv7 by default. Use UUIDv4 when compatibility
	// with a consumer that requires random UUIDs is needed.
	// rotator.RecordIDVersion = warc.UUIDv4

	// Configure HTTP client settings
	clientSettings := warc.HTTPClientSettings{
		RotatorSettings: rotator,
		TempDir:         "./temp",
		EnableHTTP2:     true,
		// Keepalive is enabled by default. Set DisableKeepAlives only when
		// connection reuse is intentionally unwanted.
	}

	// Create HTTP client
	client, err := warc.NewWARCWritingHTTPClient(clientSettings)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://archive.org", nil)
	if err != nil {
		panic(err)
	}
	exchange, startErr := client.Start(req)
	if exchange == nil {
		panic(startErr)
	}
	// Releases the capture on every early-return path. Once Commit succeeds,
	// this deferred Discard cannot change the decision.
	defer exchange.Discard(context.Background())
	if startErr != nil {
		// Commit includes the Start error in its complete result. Do not join
		// startErr again, or the same transport failure will be printed twice.
		_, err := exchange.Commit(context.Background())
		panic(err)
	}
	// Process response
	_, _ = io.Copy(io.Discard, exchange.Response.Body)
	_ = exchange.Response.Body.Close()
	// Keep the exchange and wait until its records reach the WARC writer.
	if _, err := exchange.Commit(context.Background()); err != nil {
		panic(err)
	}
	finalized, err := client.Shutdown(context.Background())
	if err != nil {
		panic(err)
	}
	_ = finalized.FinalizedFiles
}
```

HTTP/1 captures contain the plaintext HTTP/1 wire bytes seen by the transport.
HTTP/2 and HTTP/3 captures are deterministic `application/http`
serializations of the actual stream headers, body data, and trailers.
Closing a response body early performs a bounded drain; if the message boundary
cannot be reached, `Exchange.Commit` reports a truncated attempt. To reject a
capture after inspecting its response, call `Exchange.Discard`; it closes any
unread response body and releases the captured temporary data.

HTTP exchanges are written in request-then-response order by default. Set
`rotator.UseInternetArchiveRecordOrder = true` for IA-compatible
response-then-request order.

## CLI Tools

In addition to the Go library, gowarc provides several command-line utilities for working with WARC files:

### Installation

Pre-built releases are available on the [GitHub releases page](https://github.com/internetarchive/gowarc/releases).

```bash
# Install from source
go install github.com/internetarchive/gowarc/cmd/warc@latest

# Or build locally
cd cmd/warc/
go build -o warc
```

### Available Commands

#### `warc extract`
Extract files and content from WARC archives with filtering options.

```bash
# Extract all files from WARC archives
warc extract file1.warc.gz file2.warc.gz

# Extract only specific content types
warc extract --content-type "text/html" --content-type "image/jpeg" archive.warc.gz

# Extract to specific directory with multiple threads  
warc extract --output ./extracted --threads 4 *.warc.gz

# Sort extracted files by host
warc extract --host-sort archive.warc.gz
```

#### `warc mend` 
Repair and close incomplete gzip-compressed WARC files that were left with `.open` suffix during crawling.

```bash
# Dry run to see what would be fixed
warc mend --dry-run *.warc.gz.open

# Fix files with confirmation prompts  
warc mend corrupted.warc.gz.open

# Auto-fix without prompts
warc mend --yes *.warc.gz.open

# Force verification of any gzip WARC files (not just .open)
warc mend --force --dry-run archive.warc.gz
```

**Features:**
- By default, only processes `.open` files; use `--force` to verify any gzip WARC files
- Verifies gzip format using magic bytes, not just file extension
- Detects and removes trailing garbage bytes
- Truncates at corruption points while preserving maximum valid data  
- Removes `.open` suffix to "close" files when present
- Provides comprehensive statistics on repairs performed
- Memory-efficient streaming for large files

See [cmd/warc/mend/README.md](cmd/warc/mend/README.md) for detailed documentation.

#### `warc verify`
Validate the integrity and structure of WARC files.

```bash
# Verify single file
warc verify archive.warc.gz

# Verify multiple files with progress
warc verify -v *.warc.gz

# JSON output for automation
warc verify --json archive.warc.gz
```

#### `warc completion`
Generate shell completion scripts for bash, zsh, fish, or PowerShell.

```bash
# Bash completion
warc completion bash > /etc/bash_completion.d/warc

# Zsh completion
warc completion zsh > ~/.zsh/completions/_warc

# Fish completion
warc completion fish > ~/.config/fish/completions/warc.fish

# PowerShell completion
warc completion powershell > warc.ps1
```

### Global Flags

All commands support these global options:

- `-v, --verbose` - Enable verbose/debug logging
- `--json` - Output logs in JSON format for structured processing
- `-h, --help` - Show help for any command

## Build tags

- `standard_gzip`: Use the standard library gzip implementation instead of the faster one from [klauspost](https://github.com/klauspost/compress)
- `klauspost_gzip`: Use the faster gzip implementation from [klauspost](https://github.com/klauspost/compress) (default, you don't need to specify it)

## License

This module is released under CC0 license.
You can find a copy of the CC0 License in the [LICENSE](./LICENSE) file.
