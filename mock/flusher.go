// Package mock provides a mock implementation of a batch handler.
package mock

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Flusher is a mock implementation of a batch handler.
// It exposes a block and unblock method to influence the flushing behavior.
type Flusher[T any] struct {
	sync.Mutex

	// onFlush is called when the flush method is called.
	// You can overwrite it in a test.
	onFlush func(ctx context.Context, items []T)

	blockers        int
	blockersChanged chan struct{}

	batchesFlushed   [][]T
	flushStartCount  int
	flushFinishCount int

	flushWasInterrupted bool

	// flushStartCallbacks is a list of callbacks that are called when the flush method starts.
	// If the callback returns true, it is removed from the list.
	flushStartCallbacks []func() bool
	// flushFinishCallbacks is a list of callbacks that are called when the flush method finishes.
	// If the callback returns true, it is removed from the list.
	flushFinishCallbacks []func() bool

	// contextCancelGracePeriod is the time to wait for the flusher to finish before shutting down because of
	// a context cancellation.
	contextCancelGracePeriod time.Duration
}

// SetIgnoreContextCancellation sets the time to wait for the flusher to finish before shutting down because of
// a context cancellation.
func (f *Flusher[T]) SetIgnoreContextCancellation(d time.Duration) {
	f.Lock()
	defer f.Unlock()

	f.contextCancelGracePeriod = d
}

// Flush is the handler for flushing items.
// In this mock implementation, it will block until all blockers are removed.
func (f *Flusher[T]) Flush(ctx context.Context, items []T) {
	f.Lock()

	// fmt.Printf("Flushing %d items, err %v\n", len(items), ctx.Err())

	if f.contextCancelGracePeriod > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), f.contextCancelGracePeriod)
		defer cancel()
	}

	// Lazy init, so we don't need a constructor..
	if f.blockersChanged == nil {
		f.blockersChanged = make(chan struct{})
	}

	f.flushStartCount++
	for i := 0; i < len(f.flushStartCallbacks); i++ {
		if f.flushStartCallbacks[i]() {
			f.flushStartCallbacks = append(f.flushStartCallbacks[:i], f.flushStartCallbacks[i+1:]...)
			i--
		}
	}

	// The blocking logic:
	// * If there are blockers, we wait until they are all removed.
	// * If the context is cancelled, we stop waiting.
	for {
		if f.blockers == 0 || ctx.Err() != nil {
			break
		}
		blockersChanged := f.blockersChanged
		f.Unlock()
		select {
		case <-ctx.Done():
			f.flushWasInterrupted = true
			return
		case <-blockersChanged:
		}
		f.Lock()
	}

	if ctx.Err() != nil {
		f.flushWasInterrupted = true
		f.Unlock()
		return
	}

	if f.onFlush != nil {
		f.onFlush(ctx, items)
	}

	f.flushFinishCount++
	f.batchesFlushed = append(f.batchesFlushed, items)

	for i := 0; i < len(f.flushFinishCallbacks); i++ {
		if f.flushFinishCallbacks[i]() {
			f.flushFinishCallbacks = append(f.flushFinishCallbacks[:i], f.flushFinishCallbacks[i+1:]...)
			i--
		}
	}

	f.Unlock()
}

// SetOnFlush sets a callback that is called when the flush method is called.
func (f *Flusher[T]) SetOnFlush(onFlush func(ctx context.Context, items []T)) {
	f.Lock()
	defer f.Unlock()

	f.onFlush = onFlush
}

// BatchesFlushed returns all batches that have been flushed.
func (f *Flusher[T]) BatchesFlushed() [][]T {
	f.Lock()
	defer f.Unlock()

	return f.batchesFlushed
}

// Block any calls to the flusher.
func (f *Flusher[T]) Block() {
	f.Lock()
	defer f.Unlock()

	f.blockers++
}

// Unblock the flusher.
func (f *Flusher[T]) Unblock() {
	f.Lock()
	defer f.Unlock()

	f.blockers--
	if f.blockersChanged != nil {
		close(f.blockersChanged)
	}
	f.blockersChanged = make(chan struct{})
}

// StartCount returns the number of times the flush method has been called.
func (f *Flusher[T]) StartCount() int {
	f.Lock()
	defer f.Unlock()

	return f.flushStartCount
}

// FinishCount returns the number of times the flush method has finished.
func (f *Flusher[T]) FinishCount() int {
	f.Lock()
	defer f.Unlock()

	return f.flushFinishCount
}

// WasInterrupted returns whether the flush was interrupted due to a context cancellation.
func (f *Flusher[T]) WasInterrupted() bool {
	f.Lock()
	defer f.Unlock()

	return f.flushWasInterrupted
}

// IsFlushingRightNow returns whether the flusher is currently running.
func (f *Flusher[T]) IsFlushingRightNow() bool {
	f.Lock()
	defer f.Unlock()

	return f.flushFinishCount < f.flushStartCount && !f.flushWasInterrupted
}

// BlockForDuration blocks the flusher for a given duration.
func (f *Flusher[T]) BlockForDuration(duration int) {
	f.Block()
	go func() {
		<-time.After(time.Millisecond * time.Duration(duration))
		f.Unblock()
	}()
}

func (f *Flusher[T]) waitForFlushesOp(count int, operation string, timeout ...time.Duration) chan struct{} {
	f.Lock()
	defer f.Unlock()

	// This saves you from needing to wait a long time in case you make a mistake in your test.
	waitDuration := time.Second * 5
	if len(timeout) > 0 {
		waitDuration = timeout[0]
	}

	// We collect the stack so we have a pointer to the actual test, inside that `go func` call below
	// you would otherwise have no idea where the panic came from.
	stack := debug.Stack()

	criteriaMet := atomic.Bool{}
	go func() {
		<-time.After(waitDuration)
		if !criteriaMet.Load() {
			panic(fmt.Sprintf(
				"timeout waiting for flushes, test is likely incorrect [op %s, count %d]\n%s", operation, count, stack))
		}
	}()

	ch := make(chan struct{})
	var callbacks []func() bool
	switch operation {
	case "start":
		if f.flushStartCount >= count {
			close(ch)
			return ch
		}
		callbacks = f.flushStartCallbacks
		defer func() {
			f.flushStartCallbacks = callbacks
		}()
	case "finish":
		if f.flushFinishCount >= count {
			close(ch)
			return ch
		}
		callbacks = f.flushFinishCallbacks
		defer func() {
			f.flushFinishCallbacks = callbacks
		}()
	}

	callbacks = append(callbacks, func() bool {
		switch operation {
		case "start":
			if f.flushStartCount >= count {
				criteriaMet.Store(true)
			}
		case "finish":
			if f.flushFinishCount >= count {
				criteriaMet.Store(true)
			}
		}

		if criteriaMet.Load() {
			close(ch)
			return true
		}
		return false
	})

	return ch
}

// WaitForTotalFinished returns a channel that is closed when the flusher has finished flushing a given number
// of times since the start of the flusher.
// If the flusher does not finish in the given timeout, a panic is triggered. By default the timeout is 1 second.
func (f *Flusher[T]) WaitForTotalFinished(num int, timeout ...time.Duration) chan struct{} {
	return f.waitForFlushesOp(num, "finish", timeout...)
}

// WaitForTotalStarted returns a channel that is closed when the flusher has started flushing a given number of
// times since the start of the flusher.
// If the flusher does not start in the given timeout, a panic is triggered. By default the timeout is 1 second.
func (f *Flusher[T]) WaitForTotalStarted(num int, timeout ...time.Duration) chan struct{} {
	return f.waitForFlushesOp(num, "start", timeout...)
}
