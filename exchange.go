package warc

import (
	"context"
	"errors"
	"sync"

	http "github.com/saveweb/fhttp"
)

type exchangeContextKey struct{}
type writerOwnerContextKey struct{}

// AttemptResult describes the archival outcome of one actual transport attempt.
// A retried logical request can therefore contain more than one result.
type AttemptResult struct {
	Protocol string
	Outcome  http.CaptureOutcome
	Records  FeedbackEvent
	Err      error
}

// ExchangeResult is complete only after every transport attempt has finished
// and every retained WARC batch has been accepted by a writer.
type ExchangeResult struct {
	Attempts []AttemptResult
	Records  FeedbackEvent
	Err      error
}

// Exchange separates receiving response headers from durable archival
// completion. Callers consume or close Response.Body, then call Wait.
type Exchange struct {
	Response *http.Response
	state    *exchangeState
}

func (e *Exchange) Wait(ctx context.Context) (ExchangeResult, error) {
	if e == nil || e.state == nil {
		return ExchangeResult{}, errors.New("warc: nil exchange")
	}
	select {
	case <-e.state.done:
		result := e.state.result()
		return result, result.Err
	case <-ctx.Done():
		return ExchangeResult{}, context.Cause(ctx)
	}
}

type exchangeState struct {
	client       *CustomHTTPClient
	feedback     chan FeedbackEvent
	mu           sync.Mutex
	active       int
	networkDone  bool
	networkErr   error
	attempts     []AttemptResult
	done         chan struct{}
	completeOnce sync.Once
}

func newExchangeState(client *CustomHTTPClient, feedback chan FeedbackEvent) *exchangeState {
	return &exchangeState{client: client, feedback: feedback, done: make(chan struct{})}
}

func exchangeStateFromContext(ctx context.Context) *exchangeState {
	state, _ := ctx.Value(exchangeContextKey{}).(*exchangeState)
	return state
}

func (s *exchangeState) beginAttempt() {
	s.mu.Lock()
	s.active++
	s.mu.Unlock()
	s.client.WaitGroup.Add(1)
}

func (s *exchangeState) finishAttempt(result AttemptResult) {
	s.mu.Lock()
	s.attempts = append(s.attempts, result)
	s.active--
	s.completeLocked()
	s.mu.Unlock()
	s.client.WaitGroup.Done()
}

func (s *exchangeState) finishNetwork(err error) {
	s.mu.Lock()
	s.networkDone = true
	s.networkErr = err
	s.completeLocked()
	s.mu.Unlock()
}

func (s *exchangeState) completeLocked() {
	// Network completion alone is insufficient: retries can leave capture
	// serialization and writer acknowledgement running asynchronously.
	if !s.networkDone || s.active != 0 {
		return
	}
	s.completeOnce.Do(func() {
		if s.feedback != nil {
			events := append(FeedbackEvent(nil), s.resultLocked().Records...)
			if len(events) > 0 {
				s.feedback <- events
			}
			close(s.feedback)
		}
		close(s.done)
	})
}

func (s *exchangeState) result() ExchangeResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resultLocked()
}

func (s *exchangeState) resultLocked() ExchangeResult {
	result := ExchangeResult{
		Attempts: append([]AttemptResult(nil), s.attempts...),
		Err:      s.networkErr,
	}
	for _, attempt := range s.attempts {
		result.Records = append(result.Records, attempt.Records...)
		result.Err = errors.Join(result.Err, attempt.Err)
	}
	return result
}
