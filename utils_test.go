package warc

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Tests for the NewRotatorSettings function
func TestNewRotatorSettings(t *testing.T) {
	rotatorSettings := NewRotatorSettings("test.local")

	if rotatorSettings.Prefix != "WARC" {
		t.Error("Failed to set WARC rotator's filename prefix")
	}

	if rotatorSettings.WARCSize != 1000 {
		t.Error("Failed to set WARC rotator's WARC size")
	}

	if rotatorSettings.OutputDirectory != "./" {
		t.Error("Failed to set WARC rotator's output directory")
	}

	if rotatorSettings.Compression != CompressionGzip {
		t.Error("Failed to set WARC rotator's compression algorithm")
	}

	if rotatorSettings.CompressionDictionary != "" {
		t.Error("Failed to set WARC rotator's compression dictionary")
	}

	if rotatorSettings.RecordIDVersion != UUIDv7 {
		t.Errorf("RecordIDVersion = %q, want %q", rotatorSettings.RecordIDVersion, UUIDv7)
	}
}

func TestWriterRecordIDVersion(t *testing.T) {
	for _, test := range []struct {
		name    string
		version UUIDVersion
		want    uuid.Version
	}{
		{name: "default", want: uuid.Version(7)},
		{name: "v7", version: UUIDv7, want: uuid.Version(7)},
		{name: "v4", version: UUIDv4, want: uuid.Version(4)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			writer, err := NewWriter(&output, "test.warc", SHA1, CompressionNone, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if test.name != "default" {
				writer.RecordIDVersion = test.version
			}
			record := NewRecord(t.TempDir())
			if _, err := io.WriteString(record.Content, "test"); err != nil {
				t.Fatal(err)
			}
			recordID, err := writer.WriteRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			id, err := uuid.Parse(recordID)
			if err != nil {
				t.Fatalf("parse record ID %q: %v", recordID, err)
			}
			if got := id.Version(); got != test.want {
				t.Errorf("UUID version = %d, want %d", got, test.want)
			}
			if got := record.Header.Get("WARC-Record-ID"); got != "<urn:uuid:"+recordID+">" {
				t.Errorf("WARC-Record-ID = %q", got)
			}
		})
	}
}

func TestCheckRotatorSettingsRecordIDVersion(t *testing.T) {
	settings := NewRotatorSettings("test.local")
	settings.RecordIDVersion = UUIDVersion("v8")
	settings.OutputDirectory = t.TempDir()
	if err := checkRotatorSettings(settings); err == nil || !strings.Contains(err.Error(), "invalid UUID version") {
		t.Fatalf("checkRotatorSettings() error = %v, want invalid UUID version", err)
	}
}

// Tests for the isLineStartingWithHTTPMethod function
func TestIsHTTPRequest(t *testing.T) {
	goodHTTPRequestHeaders := []string{
		"GET /index.html HTTP/1.1",
		"POST /api/login HTTP/1.1",
		"DELETE /api/products/456 HTTP/1.1",
		"HEAD /about HTTP/1.0",
		"OPTIONS / HTTP/1.1",
		"PATCH /api/item/789 HTTP/1.1",
		"GET /images/logo.png HTTP/1.1",
	}

	for _, header := range goodHTTPRequestHeaders {
		if !isHTTPRequest(header) {
			t.Error("Invalid HTTP Method parsing:", header)
		}
	}
}
