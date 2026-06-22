package warc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/saveweb/gowarc/pkg/spooledtempfile"
)

// splitKeyValue parses WARC record header fields.
func splitKeyValue(line string) (string, string) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], strings.TrimSpace(parts[1])
}

func isHTTPRequest(line string) bool {
	httpMethods := []string{"GET ", "HEAD ", "POST ", "PUT ", "DELETE ", "CONNECT ", "OPTIONS ", "TRACE ", "PATCH "}
	protocols := []string{"HTTP/1.0", "HTTP/1.1"}

	for _, method := range httpMethods {
		if strings.HasPrefix(line, method) {
			for _, protocol := range protocols {
				if strings.HasSuffix(line, protocol) {
					return true
				}
			}
		}
	}
	return false
}

func writeDictionaryHeader(writer io.Writer, dictionary []byte) error {
	dictionaryZstdwriter, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return err
	}
	defer dictionaryZstdwriter.Close()

	// Compress dictionary with ZSTD.
	// TODO: Option to allow uncompressed dictionary (maybe? not sure there's any need.)
	payload := dictionaryZstdwriter.EncodeAll(dictionary, nil)

	// Magic number for skippable dictionary frame (0x184D2A5D).
	// https://github.com/ArchiveTeam/wget-lua/releases/tag/v1.20.3-at.20200401.01
	// https://iipc.github.io/warc-specifications/specifications/warc-zstd/
	magic := uint32(0x184D2A5D)

	// Create the frame header (magic + payload size)
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[:4], magic)
	binary.LittleEndian.PutUint32(header[4:], uint32(len(payload)))

	// Combine header and payload together into a full frame.
	frame := append(header, payload...)

	// Write generated frame directly to WARC file.
	// The regular ZStandard writer will continue afterwards with normal ZStandard frames.
	if _, err := writer.Write(frame); err != nil {
		return err
	}

	return nil
}

// NewWriter creates a new WARC writer.
func NewWriter(writer io.Writer, fileName string, digestAlgorithm DigestAlgorithm, compression compressionType, newFileCreation bool, dictionary []byte) (*Writer, error) {
	switch compression {
	case CompressionGzip:
		gzipWriter := newGzipWriter(writer)

		return &Writer{
			FileName:        fileName,
			DigestAlgorithm: digestAlgorithm,
			Compressor:      gzipWriter,
			BufWriter:       bufio.NewWriter(gzipWriter),
		}, nil
	case CompressionZstd:
		if newFileCreation && len(dictionary) > 0 {
			if err := writeDictionaryHeader(writer, dictionary); err != nil {
				return nil, err
			}
		}

		// Create ZStandard writer either with or without the encoder dictionary and return it.
		var zstdWriter *zstd.Encoder
		var err error
		eopts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBetterCompression)}
		if len(dictionary) > 0 {
			eopts = append(eopts, zstd.WithEncoderDict(dictionary))
		}
		zstdWriter, err = zstd.NewWriter(writer, eopts...)
		if err != nil {
			return nil, err
		}

		return &Writer{
			FileName:        fileName,
			DigestAlgorithm: digestAlgorithm,
			Compressor:      zstdWriter,
			BufWriter:       bufio.NewWriter(zstdWriter),
		}, nil
	case CompressionNone:
		return &Writer{
			FileName:        fileName,
			DigestAlgorithm: digestAlgorithm,
			BufWriter:       bufio.NewWriter(writer),
		}, nil
	default:
		return nil, fmt.Errorf("invalid compression algorithm: %s", compression)
	}
}

// NewRecord creates a new WARC record.
func NewRecord(tempDir string) *Record {
	content, err := spooledtempfile.NewSpooledTempFile("warc", tempDir)
	if err != nil {
		panic(err)
	}
	return &Record{
		RecordInfo: RecordInfo{
			Header: NewHeader(),
		},
		Content: content,
	}
}

// NewRecordBatch creates a record batch, it also initialize the capture time.
func NewRecordBatch(feedbackChan chan FeedbackEvent) *RecordBatch {
	return &RecordBatch{
		CaptureTime:  time.Now().UTC().Format(time.RFC3339Nano),
		FeedbackChan: feedbackChan,
	}
}

// NewRotatorSettings creates a RotatorSettings structure
// and initialize it with default values
func NewRotatorSettings() *RotatorSettings {
	return &RotatorSettings{
		WarcinfoContent:       NewHeader(),
		Prefix:                "WARC",
		WARCSize:              1000,
		Compression:           CompressionGzip,
		digestAlgorithm:       SHA1,
		CompressionDictionary: "",
		OutputDirectory:       "./",
	}
}

// checkRotatorSettings validate RotatorSettings settings, and set
// default values if needed
func checkRotatorSettings(settings *RotatorSettings) (err error) {
	// Get host name as reported by the kernel
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// Check if output directory is specified, if not, set it to the current directory
	if settings.OutputDirectory == "" {
		settings.OutputDirectory = "./"
	} else {
		// If it is specified, check if output directory exist
		if _, err := os.Stat(settings.OutputDirectory); os.IsNotExist(err) {
			// If it doesn't exist, create it
			// MkdirAll will create all parent directories if needed
			err = os.MkdirAll(settings.OutputDirectory, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}

	if settings.WARCWriterPoolSize == 0 {
		settings.WARCWriterPoolSize = 1
	}

	// Add a trailing slash to the output directory
	if settings.OutputDirectory[len(settings.OutputDirectory)-1:] != "/" {
		settings.OutputDirectory = settings.OutputDirectory + "/"
	}

	// If prefix isn't specified, set it to "WARC"
	if settings.Prefix == "" {
		settings.Prefix = "WARC"
	}

	// If WARC size isn't specified, set it to 1GB (10^9 bytes) by default
	if settings.WARCSize == 0 {
		settings.WARCSize = 1000
	}

	// Check if the specified compression algorithm is valid
	switch settings.Compression {
	case CompressionGzip, CompressionZstd, CompressionNone:
	default:
		return fmt.Errorf("invalid compression algorithm: %s", settings.Compression)
	}

	// Add few headers to the warcinfo payload, to not have it empty
	settings.WarcinfoContent.Set("hostname", hostName)
	settings.WarcinfoContent.Set("format", "WARC file version 1.1")
	settings.WarcinfoContent.Set("conformsTo", "http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/")

	return nil
}

func getContentLength(rwsc spooledtempfile.ReadWriteSeekCloser) int64 {
	fileInfo, err := os.Stat(rwsc.Name())
	if err != nil {
		panic(err)
	}
	return fileInfo.Size()
}

func parseRequestTargetURI(scheme string, content io.ReadSeeker) (string, error) {
	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return "", errors.New("parseRequestTargetURI: seek failed: " + err.Error())
	}

	reader := bufio.NewReaderSize(content, 4096)

	const (
		stateRequestLine = iota
		stateHeaders
	)

	var (
		target      string
		host        string
		state       = stateRequestLine
		foundHost   = false
		foundTarget = false
	)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", errors.New("parseRequestTargetURI: read line failed: " + err.Error())
		}

		line = strings.TrimSpace(line)

		switch state {
		case stateRequestLine:
			if isHTTPRequest(line) {
				parts := strings.Split(line, " ")
				if len(parts) >= 2 {
					target = parts[1]
					foundTarget = true
				}
				state = stateHeaders
			}
		case stateHeaders:
			if line == "" {
				break
			}
			if strings.HasPrefix(strings.ToLower(line), "host: ") {
				host = strings.TrimSpace(line[6:])
				foundHost = true
			}
		}

		if foundHost && foundTarget {
			break
		}
	}

	if !foundTarget || !foundHost {
		return "", errors.New("parseRequestTargetURI: failed to parse host and target from request")
	}

	if strings.HasPrefix(target, scheme+"://"+host) {
		return target, nil
	}
	return scheme + "://" + host + target, nil
}

func findEndOfHeadersOffset(content io.ReadSeeker) (int, error) {
	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return -1, fmt.Errorf("FindEndOfHeadersOffset: seek failed: %w", err)
	}

	found := false
	bigBlock := make([]byte, 0, 4)
	block := make([]byte, 1)
	endOfHeadersOffset := 0

	for {
		n, err := content.Read(block)
		if n > 0 {
			switch len(bigBlock) {
			case 0:
				if string(block) == "\r" {
					bigBlock = append(bigBlock, block...)
				}
			case 1:
				if string(block) == "\n" {
					bigBlock = append(bigBlock, block...)
				} else {
					bigBlock = nil
				}
			case 2:
				if string(block) == "\r" {
					bigBlock = append(bigBlock, block...)
				} else {
					bigBlock = nil
				}
			case 3:
				if string(block) == "\n" {
					bigBlock = append(bigBlock, block...)
					found = true
				} else {
					bigBlock = nil
				}
			}

			endOfHeadersOffset++

			if found {
				break
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return -1, err
		}
	}

	if !found {
		return -1, errors.New("FindEndOfHeadersOffset: could not find the end of the headers")
	}

	return endOfHeadersOffset, nil
}
