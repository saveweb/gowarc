package warc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	http "github.com/saveweb/fhttp"
	"github.com/saveweb/gowarc/pkg/spooledtempfile"
)

func buildRequestRecord(scheme string, client *CustomHTTPClient, source spooledtempfile.ReadWriteSeekCloser) (*Record, string, error) {
	record, err := newRecord(client.TempDir)
	if err != nil {
		return nil, "", fmt.Errorf("create request record: %w", err)
	}
	failed := true
	defer func() {
		_ = source.Close()
		if failed {
			_ = record.Content.Close()
		}
	}()
	record.Header.Set("WARC-Type", "request")
	record.Header.Set("Content-Type", "application/http; msgtype=request")
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return nil, "", fmt.Errorf("seek request capture: %w", err)
	}
	if _, err := io.Copy(record.Content, source); err != nil {
		return nil, "", fmt.Errorf("copy request capture: %w", err)
	}
	target, err := parseRequestTargetURI(scheme, record.Content)
	if err != nil {
		return nil, "", err
	}
	failed = false
	return record, target, nil
}

func buildResponseRecord(ctx context.Context, client *CustomHTTPClient, source spooledtempfile.ReadWriteSeekCloser, target, requestMethod string, truncated bool) (*Record, error) {
	record, err := newRecord(client.TempDir)
	if err != nil {
		return nil, fmt.Errorf("create response record: %w", err)
	}
	failed := true
	defer func() {
		_ = source.Close()
		if failed {
			_ = record.Content.Close()
		}
	}()
	record.Header.Set("WARC-Type", "response")
	record.Header.Set("Content-Type", "application/http; msgtype=response")
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek response capture: %w", err)
	}
	bytesCopied, err := io.Copy(record.Content, source)
	if err != nil {
		return nil, fmt.Errorf("copy response capture: %w", err)
	}
	if _, err := record.Content.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek response for parsing: %w", err)
	}
	responseReader := bufio.NewReader(record.Content)
	request := &http.Request{Method: requestMethod}
	var resp *http.Response
	// application/http may contain one or more informational responses before
	// the final response whose entity body determines the payload digest.
	for {
		resp, err = http.ReadResponse(responseReader, request)
		if err != nil {
			if truncated {
				failed = false
				return record, nil
			}
			return nil, fmt.Errorf("parse captured response: %w", err)
		}
		if resp.StatusCode < 100 || resp.StatusCode >= 200 || resp.StatusCode == http.StatusSwitchingProtocols {
			break
		}
		if err := resp.Body.Close(); err != nil {
			return nil, err
		}
	}
	if truncated {
		// A partial response remains useful evidence, but its payload boundary is
		// unknown, so it is ineligible for payload digest and revisit dedupe.
		failed = false
		return record, nil
	}
	payloadDigest, err := GetDigest(resp.Body, client.DigestAlgorithm)
	closeErr := resp.Body.Close()
	if err != nil && !(truncated && errors.Is(err, io.ErrUnexpectedEOF)) {
		return nil, errors.Join(err, closeErr)
	}
	if closeErr != nil && !(truncated && errors.Is(closeErr, io.ErrUnexpectedEOF)) {
		return nil, closeErr
	}
	if err == nil {
		record.Header.Set("WARC-Payload-Digest", payloadDigest)
	}

	var revisit revisitRecord
	if payloadDigest != "" && bytesCopied >= int64(client.dedupeOptions.SizeThreshold) && !slicesContains(emptyPayloadDigests, payloadDigest) {
		if client.dedupeOptions.LocalDedupe {
			revisit, _ = client.dedupeHashTable.Get(payloadDigest)
			if revisit.targetURI != "" {
				LocalDedupeTotalBytes.Add(int64(revisit.size))
				LocalDedupeTotal.Add(1)
			}
		}
		if client.dedupeOptions.DoppelgangerDedupe && client.DigestAlgorithm == SHA1 && revisit.targetURI == "" {
			revisit, _ = checkDoppelgangerRevisit(client.dedupeOptions.DoppelgangerHost, payloadDigest, target)
			if revisit.targetURI != "" {
				DoppelgangerDedupeTotalBytes.Add(bytesCopied)
				DoppelgangerDedupeTotal.Add(1)
			}
		}
		if client.dedupeOptions.CDXDedupe && client.DigestAlgorithm == SHA1 && revisit.targetURI == "" {
			revisit, _ = checkCDXRevisit(client.dedupeOptions.CDXURL, payloadDigest, target, client.dedupeOptions.CDXCookie)
			if revisit.targetURI != "" {
				CDXDedupeTotalBytes.Add(bytesCopied)
				CDXDedupeTotal.Add(1)
			}
		}
	}
	if revisit.targetURI != "" && !slicesContains(emptyPayloadDigests, payloadDigest) {
		record.Header.Set("WARC-Type", "revisit")
		record.Header.Set("WARC-Refers-To-Target-URI", revisit.targetURI)
		record.Header.Set("WARC-Refers-To-Date", revisit.date.Format(time.RFC3339Nano))
		if revisit.responseUUID != "" {
			record.Header.Set("WARC-Refers-To", "<urn:uuid:"+revisit.responseUUID+">")
		}
		record.Header.Set("WARC-Profile", "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest")
		record.Header.Set("WARC-Truncated", "length")
		end, err := findEndOfHeadersOffset(record.Content)
		if err != nil || end == -1 {
			return nil, errors.Join(err, errors.New("captured response has no header terminator"))
		}
		headers, err := spooledtempfile.NewSpooledTempFile("warc", client.TempDir)
		if err != nil {
			return nil, err
		}
		if _, err := record.Content.Seek(0, io.SeekStart); err != nil {
			_ = headers.Close()
			return nil, err
		}
		if _, err := io.CopyN(headers, record.Content, int64(end)); err != nil {
			_ = headers.Close()
			return nil, err
		}
		if err := record.Content.Close(); err != nil {
			_ = headers.Close()
			return nil, err
		}
		record.Content = headers
	}
	failed = false
	return record, nil
}

func slicesContains[S ~[]E, E comparable](s S, v E) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}
