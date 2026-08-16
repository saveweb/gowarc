package warc

import (
	"context"
	"errors"
	"sync"

	http "github.com/saveweb/fhttp"
)

type exchangeContextKey struct{}
type writerOwnerContextKey struct{}

var ErrExchangeAlreadyDecided = errors.New("warc: exchange already has a different decision")

type exchangeDecision uint8

const (
	exchangeUndecided exchangeDecision = iota
	exchangeCommit
	exchangeDiscard
)

// AttemptResult describes the archival outcome of one actual transport attempt.
// A retried logical request can therefore contain more than one result.
type AttemptResult struct {
	Protocol   string
	Outcome    http.CaptureOutcome
	Records    FeedbackEvent
	Err        error
	cleanupErr error
}

// ExchangeResult is complete only after every transport attempt has finished
// and every retained WARC batch has been accepted by a writer.
type ExchangeResult struct {
	Attempts []AttemptResult
	Records  FeedbackEvent
	Err      error
}

// Exchange separates receiving response headers from the decision to retain
// the capture. Callers consume or close Response.Body, then finish with either
// Commit or Discard.
type Exchange struct {
	Response *http.Response
	state    *exchangeState
}

// Commit retains every transport attempt belonging to the logical request and
// waits until its records have been accepted by a WARC writer. Repeating Commit
// is harmless and returns the same result.
func (e *Exchange) Commit(ctx context.Context) (ExchangeResult, error) {
	if err := e.decide(exchangeCommit); err != nil {
		return ExchangeResult{}, err
	}
	return e.wait(ctx)
}

// Discard closes any response body and releases every transport attempt
// without writing HTTP records. Repeating Discard is harmless.
func (e *Exchange) Discard(ctx context.Context) error {
	if e == nil || e.state == nil {
		return errors.New("warc: nil exchange")
	}
	if err := e.state.discard(); err != nil {
		return err
	}
	_, err := e.wait(ctx)
	return err
}

func (e *Exchange) decide(decision exchangeDecision) error {
	if e == nil || e.state == nil {
		return errors.New("warc: nil exchange")
	}
	return e.state.decide(decision)
}

func (e *Exchange) wait(ctx context.Context) (ExchangeResult, error) {
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
	client           *CustomHTTPClient
	feedback         chan FeedbackEvent
	mu               sync.Mutex
	active           int
	networkDone      bool
	networkErr       error
	attempts         []AttemptResult
	decision         exchangeDecision
	decisionDone     chan struct{}
	response         *http.Response
	responseClosed   bool
	responseCloseErr error
	done             chan struct{}
	decisionOnce     sync.Once
	completeOnce     sync.Once
}

func newExchangeState(client *CustomHTTPClient, feedback chan FeedbackEvent, decision exchangeDecision) *exchangeState {
	state := &exchangeState{
		client:       client,
		feedback:     feedback,
		decisionDone: make(chan struct{}),
		done:         make(chan struct{}),
	}
	if decision != exchangeUndecided {
		state.decision = decision
		close(state.decisionDone)
	}
	return state
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

func (s *exchangeState) decide(decision exchangeDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.decision != exchangeUndecided {
		if s.decision == decision {
			return nil
		}
		return ErrExchangeAlreadyDecided
	}
	s.decision = decision
	s.decisionOnce.Do(func() { close(s.decisionDone) })
	s.completeLocked()
	return nil
}

func (s *exchangeState) discard() error {
	s.mu.Lock()
	if s.decision == exchangeCommit {
		s.mu.Unlock()
		return ErrExchangeAlreadyDecided
	}
	if s.decision == exchangeUndecided {
		s.decision = exchangeDiscard
		s.decisionOnce.Do(func() { close(s.decisionDone) })
		s.completeLocked()
	}
	response := s.responseToCloseLocked()
	s.mu.Unlock()
	s.closeResponse(response)
	return nil
}

func (s *exchangeState) closeForShutdown() (networkPending bool) {
	s.mu.Lock()
	if s.decision == exchangeUndecided {
		s.decision = exchangeDiscard
		s.decisionOnce.Do(func() { close(s.decisionDone) })
		s.completeLocked()
	}
	response := s.responseToCloseLocked()
	networkPending = !s.networkDone
	s.mu.Unlock()
	s.closeResponse(response)
	return networkPending
}

func (s *exchangeState) waitForDecision() exchangeDecision {
	<-s.decisionDone
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decision
}

func (s *exchangeState) attachResponse(response *http.Response) {
	s.mu.Lock()
	s.response = response
	var closeResponse *http.Response
	if s.decision == exchangeDiscard {
		closeResponse = s.responseToCloseLocked()
	}
	s.mu.Unlock()
	s.closeResponse(closeResponse)
}

func (s *exchangeState) responseToCloseLocked() *http.Response {
	if s.response == nil || s.response.Body == nil || s.responseClosed {
		return nil
	}
	s.responseClosed = true
	return s.response
}

func (s *exchangeState) closeResponse(response *http.Response) {
	if response == nil {
		return
	}
	err := response.Body.Close()
	s.mu.Lock()
	s.responseCloseErr = errors.Join(s.responseCloseErr, err)
	s.mu.Unlock()
}

func (s *exchangeState) completeLocked() {
	// Network completion alone is insufficient: retries can leave capture
	// serialization and writer acknowledgement running asynchronously.
	if s.decision == exchangeUndecided || !s.networkDone || s.active != 0 {
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
		s.client.unregisterExchange(s)
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
	}
	if s.decision == exchangeCommit {
		result.Err = s.networkErr
	} else {
		result.Err = s.responseCloseErr
	}
	for _, attempt := range s.attempts {
		result.Records = append(result.Records, attempt.Records...)
		if s.decision == exchangeCommit {
			result.Err = errors.Join(result.Err, attempt.Err, attempt.cleanupErr)
		} else {
			result.Err = errors.Join(result.Err, attempt.cleanupErr)
		}
	}
	return result
}
