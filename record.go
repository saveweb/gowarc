package warc

import (
	"context"
	"errors"
	"io"
)

// WriteRecordContext writes one non-HTTP WARC record and returns only after the
// writer has accepted it. Once submitted, the writer result is authoritative
// even if ctx is canceled concurrently.
func (c *CustomHTTPClient) WriteRecordContext(ctx context.Context, targetURI, warcType, contentType, payloadString string, payloadReader io.Reader) (RecordEvent, error) {
	// Initialize the record
	record, err := newRecord(c.TempDir)
	if err != nil {
		return RecordEvent{}, err
	}
	owned := true
	defer func() {
		if owned {
			_ = record.Content.Close()
		}
	}()
	// Set the headers
	record.Header.Set("WARC-Type", warcType)
	record.Header.Set("WARC-Target-URI", targetURI)
	if contentType != "" {
		record.Header.Set("Content-Type", contentType)
	}
	// Write the payload
	switch {
	case payloadString != "":
		_, err = io.WriteString(record.Content, payloadString)
	case payloadReader != nil:
		_, err = io.Copy(record.Content, payloadReader)
	default:
		err = errors.New("warc: no record payload provided")
	}
	if err != nil {
		return RecordEvent{}, err
	}
	// Add it to the batch
	batch := NewRecordBatch(nil)
	batch.Records = []*Record{record}
	// Wait for the record to be written
	result, err := c.WriteBatch(ctx, batch)
	owned = false
	if err != nil {
		return RecordEvent{}, err
	}
	if len(result.Events) != 1 {
		return RecordEvent{}, errors.New("warc: writer returned an invalid record count")
	}
	return result.Events[0], nil
}

// WriteBatch submits a caller-constructed batch and waits for its writer result.
func (c *CustomHTTPClient) WriteBatch(ctx context.Context, batch *RecordBatch) (WriteResult, error) {
	if batch == nil || batch.resultDone == nil {
		return WriteResult{}, errors.New("warc: invalid record batch")
	}
	_, writerOwned := ctx.Value(writerOwnerContextKey{}).(bool)
	registered := exchangeStateFromContext(ctx) == nil && !writerOwned
	if registered {
		c.lifecycleMu.Lock()
		if c.closing {
			c.lifecycleMu.Unlock()
			return WriteResult{}, errors.New("warc: client is closing")
		}
		c.WaitGroup.Add(1)
		c.lifecycleMu.Unlock()
		defer c.WaitGroup.Done()
	}
	select {
	case c.WARCWriter <- batch:
	case <-ctx.Done():
		return WriteResult{}, context.Cause(ctx)
	}
	result, err := batch.Wait(context.Background())
	return result, err
}

// WriteRecord is retained for source compatibility. New code should use
// WriteRecordContext so archival failure is part of normal error handling.
func (c *CustomHTTPClient) WriteRecord(targetURI, warcType, contentType, payloadString string, payloadReader io.Reader) {
	if _, err := c.WriteRecordContext(context.Background(), targetURI, warcType, contentType, payloadString, payloadReader); err != nil {
		select {
		case c.ErrChan <- &Error{Err: err, Func: "WriteRecord"}:
		default:
		}
	}
}
