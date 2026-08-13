package warc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

type compressionType string

const (
	CompressionNone = compressionType("none")
	CompressionGzip = compressionType("gzip")
	CompressionZstd = compressionType("zstd")
)

var CompressionTypes = []compressionType{CompressionNone, CompressionGzip, CompressionZstd}

func MustCompressionTypeFromString(s string) compressionType {
	switch strings.ToLower(s) {
	case "none":
		return CompressionNone
	case "gzip":
		return CompressionGzip
	case "zstd":
		return CompressionZstd
	default:
		panic(fmt.Sprintf("invalid compression type: %s", s))
	}
}

// RotatorSettings is used to store the settings
// needed by recordWriter to write WARC files
type RotatorSettings struct {
	// Content of the warcinfo record that will be written
	// to all WARC files
	WarcinfoContent Header
	// Prefix used for WARC filenames, WARC 1.1 specifications
	// recommend to name files this way:
	// Prefix-Timestamp-Serial-Crawlhost.warc.gz
	Prefix string
	// Compression algorithm to use
	Compression compressionType
	// Payload digest calculation algorithm to use
	digestAlgorithm DigestAlgorithm
	// Path to a ZSTD compression dictionary to embed (and use) in .warc.zst files
	CompressionDictionary string
	// Directory where the created WARC files will be stored,
	// default will be the current directory
	OutputDirectory string
	// WARCSize is in Megabytes
	WARCSize float64
	// WARCWriterPoolSize defines the number of parallel WARC writers
	WARCWriterPoolSize int
	// RecordIDVersion selects the UUID version for generated WARC record IDs.
	// The default is UUIDv7.
	RecordIDVersion UUIDVersion
	// UseInternetArchiveRecordOrder writes each HTTP exchange as response then
	// request. The default order is request then response.
	//
	// IA has a quirk of placing response records at the beginning - They claim
	// this benefits performance, but I've thought about it for two years without
	// figuring out why.
	UseInternetArchiveRecordOrder bool
}

type writerResult struct {
	FinalizedFiles []string
	Err            error
}

type rotatorController struct {
	once     sync.Once
	terminal chan struct{}
	err      error
}

func newRotatorController() *rotatorController {
	return &rotatorController{terminal: make(chan struct{})}
}

func (c *rotatorController) fail(err error, records <-chan *RecordBatch) {
	c.once.Do(func() {
		c.err = err
		close(c.terminal)
		go func() {
			for batch := range records {
				batch.resolve(WriteResult{Err: err})
			}
		}()
	})
}

var (
	// Create a couple of counters for tracking various stats
	DataTotal atomic.Int64

	CDXDedupeTotalBytes          atomic.Int64
	DoppelgangerDedupeTotalBytes atomic.Int64
	LocalDedupeTotalBytes        atomic.Int64

	CDXDedupeTotal          atomic.Int64
	DoppelgangerDedupeTotal atomic.Int64
	LocalDedupeTotal        atomic.Int64
)

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (recordWriterChan chan *RecordBatch, doneChannels []chan bool, err error) {
	recordWriterChan, doneChannels, _, err = s.newWARCRotator()
	return recordWriterChan, doneChannels, err
}

func (s *RotatorSettings) newWARCRotator() (recordWriterChan chan *RecordBatch, doneChannels []chan bool, writerResults []<-chan writerResult, err error) {
	recordWriterChan = make(chan *RecordBatch, 1)

	// Create global atomicSerial number for numbering WARC files.
	var serial = new(atomic.Uint64)

	// Check the rotator settings and set default values
	err = checkRotatorSettings(s)
	if err != nil {
		return recordWriterChan, doneChannels, writerResults, err
	}

	var dictionary []byte

	if s.CompressionDictionary != "" {
		dictionary, err = os.ReadFile(s.CompressionDictionary)
		if err != nil {
			return recordWriterChan, doneChannels, writerResults, fmt.Errorf("read compression dictionary %q: %w", s.CompressionDictionary, err)
		}
	}

	controller := newRotatorController()
	for i := 0; i < s.WARCWriterPoolSize; i++ {
		doneChan := make(chan bool)
		resultChan := make(chan writerResult, 1)
		doneChannels = append(doneChannels, doneChan)
		writerResults = append(writerResults, resultChan)

		go recordWriter(s, recordWriterChan, doneChan, resultChan, serial, dictionary, controller)
	}

	return recordWriterChan, doneChannels, writerResults, nil
}

// reset resets the compressed writer to write to a new output.
// This reuses the encoder's internal buffers.
func (w *Writer) Reset(output io.Writer) {
	if w.Compressor != nil {
		w.Compressor.Reset(output)
		w.BufWriter.Reset(w.Compressor)
	} else {
		w.BufWriter.Reset(output)
	}
}

// Close will flush the final output and close the stream.
// The function will block until everything has been written.
// The [Compressor] can still be re-used after calling this.
// If the [Compressor] is nil, this will just flush the [Writer.BufWriter].
func (w *Writer) FlushAndCloseCompressor() (err error) {
	if w.Compressor != nil {
		err1 := w.BufWriter.Flush()
		err2 := w.Compressor.Close()
		return errors.Join(err1, err2)
	} else {
		return w.BufWriter.Flush()
	}
}

func getNextWARCFilename(outputDir, prefix string, compression compressionType, serial *atomic.Uint64, hostname string) string {
	name, err := nextWARCFilename(outputDir, prefix, compression, serial, hostname)
	if err != nil {
		panic(err)
	}
	return name
}

func nextWARCFilename(outputDir, prefix string, compression compressionType, serial *atomic.Uint64, hostname string) (string, error) {
	nextWARCFilenameWithOpenExt := generateWARCFilename(prefix, compression, serial, hostname)
	_, err := os.Stat(path.Join(outputDir, nextWARCFilenameWithOpenExt))
	for !errors.Is(err, os.ErrNotExist) {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}

		nextWARCFilenameWithOpenExt = generateWARCFilename(prefix, compression, serial, hostname)
		_, err = os.Stat(path.Join(outputDir, nextWARCFilenameWithOpenExt))
	}

	return nextWARCFilenameWithOpenExt, nil
}

func recordsInWriteOrder(records []*Record, useIAOrder bool) []*Record {
	if len(records) != 2 {
		return records
	}
	firstType := records[0].Header.Get("WARC-Type")
	secondType := records[1].Header.Get("WARC-Type")
	if !((firstType == "request" && secondType == "response") || (firstType == "response" && secondType == "request")) {
		return records
	}
	wantFirst := "request"
	if useIAOrder {
		wantFirst = "response"
	}
	if firstType == wantFirst {
		return records
	}
	// Copy before swapping so writer policy never mutates a caller-owned batch.
	ordered := append([]*Record(nil), records...)
	ordered[0], ordered[1] = ordered[1], ordered[0]
	return ordered
}

func recordWriter(settings *RotatorSettings, records chan *RecordBatch, done chan bool, result chan<- writerResult, serial *atomic.Uint64, dictionary []byte, controller *rotatorController) {
	var finalized []string
	finish := func(err error) {
		result <- writerResult{FinalizedFiles: finalized, Err: err}
		close(result)
		close(done)
	}
	currentFileNameWithOpenExt, err := nextWARCFilename(settings.OutputDirectory, settings.Prefix, settings.Compression, serial, settings.WarcinfoContent.Get("hostname"))
	if err != nil {
		controller.fail(err, records)
		finish(err)
		return
	}
	var currentWarcinfoRecordID string

	// Create and open the initial file
	warcFile, err := os.Create(filepath.Join(settings.OutputDirectory, currentFileNameWithOpenExt))
	if err != nil {
		controller.fail(err, records)
		finish(err)
		return
	}

	// Initialize WARC writer (write dictionary if specified)
	warcWriter, err := NewWriter(warcFile, currentFileNameWithOpenExt, settings.digestAlgorithm, settings.Compression, true, dictionary)
	if err != nil {
		_ = warcFile.Close()
		controller.fail(err, records)
		finish(err)
		return
	}
	warcWriter.RecordIDVersion = settings.RecordIDVersion

	// Write the info record
	currentWarcinfoRecordID, err = warcWriter.WriteInfoRecord(settings.WarcinfoContent)
	if err != nil {
		_ = warcFile.Close()
		controller.fail(err, records)
		finish(err)
		return
	}

	for {
		var recordBatch *RecordBatch
		var more bool
		select {
		case <-controller.terminal:
			_ = warcFile.Close()
			finish(controller.err)
			return
		case recordBatch, more = <-records:
		}
		if more {
			select {
			case <-controller.terminal:
				recordBatch.resolve(WriteResult{Err: controller.err})
				_ = warcFile.Close()
				finish(controller.err)
				return
			default:
			}
			if isFileSizeExceeded(warcFile, settings.WARCSize) {
				// WARC file size exceeded settings.WarcSize
				// We flush the data and close the file
				err = warcWriter.FlushAndCloseCompressor()
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					_ = warcFile.Close()
					finish(err)
					return
				}

				err = warcFile.Close()
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					finish(err)
					return
				}
				// The WARC file is renamed to remove the .open suffix
				err := os.Rename(filepath.Join(settings.OutputDirectory, currentFileNameWithOpenExt), strings.TrimSuffix(filepath.Join(settings.OutputDirectory, currentFileNameWithOpenExt), ".open"))
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					finish(err)
					return
				}
				finalized = append(finalized, strings.TrimSuffix(currentFileNameWithOpenExt, ".open"))

				// Create the new file and automatically increment the serial inside of GenerateWarcFileName
				currentFileNameWithOpenExt, err = nextWARCFilename(settings.OutputDirectory, settings.Prefix, settings.Compression, serial, settings.WarcinfoContent.Get("hostname"))
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					finish(err)
					return
				}
				warcFile, err = os.Create(filepath.Join(settings.OutputDirectory, currentFileNameWithOpenExt))
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					finish(err)
					return
				}

				// Initialize new WARC writer
				warcWriter, err = NewWriter(warcFile, currentFileNameWithOpenExt, settings.digestAlgorithm, settings.Compression, true, dictionary)
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					_ = warcFile.Close()
					finish(err)
					return
				}
				warcWriter.RecordIDVersion = settings.RecordIDVersion

				// Write the info record
				currentWarcinfoRecordID, err = warcWriter.WriteInfoRecord(settings.WarcinfoContent)
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					_ = warcFile.Close()
					finish(err)
					return
				}
			}

			orderedRecords := recordsInWriteOrder(recordBatch.Records, settings.UseInternetArchiveRecordOrder)
			recordsEvents := make([]RecordEvent, 0, len(orderedRecords))
			// Write all the records of the record batch
			for _, record := range orderedRecords {
				warcWriter.Reset(warcFile)

				record.Header.Set("WARC-Date", recordBatch.CaptureTime)
				record.Header.Set("WARC-Warcinfo-ID", "<urn:uuid:"+currentWarcinfoRecordID+">")

				_, err := warcWriter.WriteRecord(record)
				if err != nil {
					recordBatch.resolve(WriteResult{Err: err})
					controller.fail(err, records)
					_ = warcFile.Close()
					finish(err)
					return
				}
				recordsEvents = append(recordsEvents, RecordEvent{RecordInfo: record.RecordInfo, WARCFilename: strings.TrimSuffix(currentFileNameWithOpenExt, ".open")})
			}

			recordBatch.resolve(WriteResult{Events: recordsEvents})
		} else {
			// Channel has been closed
			// We flush the data, close the file, and rename it
			err = warcWriter.FlushAndCloseCompressor()
			if err != nil {
				finish(err)
				return
			}

			err = warcFile.Close()
			if err != nil {
				finish(err)
				return
			}

			// The WARC file is renamed to remove the .open suffix
			fullPath := filepath.Join(settings.OutputDirectory, currentFileNameWithOpenExt)
			err := os.Rename(fullPath, strings.TrimSuffix(fullPath, ".open"))
			if err != nil {
				finish(err)
				return
			}
			finalized = append(finalized, strings.TrimSuffix(currentFileNameWithOpenExt, ".open"))

			finish(nil)
			return
		}
	}
}
