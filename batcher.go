// Package batchman provides an in-memory batching mechanism for items of a given type.
package batchman

import (
	"context"
	"sync/atomic"
	"time"
)

// Batcher is a controller that batches items of a given type into batches with a maximum size or after a maximum delay.
type Batcher[T any] struct {
	buffer  chan T
	stopped chan struct{}

	isStopped atomic.Bool
}

// Done returns a channel that is closed when the batcher has stopped completely.
// Once a batcher has stopped, no more items can be pushed, and it can not be started again.
//
// The batcher stops when the parent context is cancelled, but it will flush the remaining items in the buffer.
// This means that the stopped channel is closed after the last item has been flushed. Depending on the implementation
// of the flush function, this might take some time.
func (b *Batcher[T]) Done() <-chan struct{} {
	return b.stopped
}

//nolint:gocognit // Splitting it up would make it harder to read.
func (b *Batcher[T]) start(ctx context.Context,
	maxDelay time.Duration,
	maxSize int,
	flush func(ctx context.Context, items []T),
) {
	timerIsRunning := false
	isCancelled := false

	var items []T
	timer := time.NewTimer(0)
	<-timer.C // See https://github.com/golang/go/issues/12721 why this is necessary.

	for {
		var (
			isMaxDelay bool
			isMaxSize  bool
		)

		if !isCancelled {
			select {
			case <-ctx.Done():
				isCancelled = true
				b.isStopped.Store(true)

				// We can cancel the timer here, because we are not going to use it anymore.
				timer.Stop()
			case <-timer.C:
				isMaxDelay = true
				timerIsRunning = false
			case item := <-b.buffer:
				items = append(items, item)
				isMaxSize = len(items) >= maxSize
			}
		} else {
			// When the context is cancelled, we need to flush the remaining items if any are present in the buffer
			select {
			case item := <-b.buffer:
				items = append(items, item)
				isMaxSize = len(items) >= maxSize
			default:
				// If no items in buffer carry on.
				// Possibly this default case is not needed - but better safe than sorry.
			}
		}

		shouldFlush := isCancelled || isMaxDelay || isMaxSize

		if !shouldFlush {
			// After the first item is added, start the timer.
			if len(items) == 1 {
				timer.Reset(maxDelay)
				timerIsRunning = true
			}
			continue
		}

		// If the batcher is cancelled and the buffer is not empty, we want to flush the
		// remaining items with the maximum batch size, so we skip until we reach max size or the buffer is empty.
		skipFlush := (isCancelled && len(b.buffer) > 0 && !isMaxSize) || len(items) == 0

		if !skipFlush {
			// We need to copy the slice to make sure that the slice that is passed is valid even if asynchronously
			// accessed in a routine spawned by the flush function.
			flushItems := make([]T, len(items))
			copy(flushItems, items)

			flush(ctx, flushItems)
			items = items[:0]

			if !isCancelled {
				if timerIsRunning && !timer.Stop() {
					<-timer.C
				}
			}
		}

		if isCancelled {
			// If the buffer is not empty, we continue flushing.
			// This loop will run again and we'll read the next item from the buffer.
			if len(b.buffer) > 0 {
				continue
			}

			close(b.stopped)
			return
		}
	}
}

// Push an item to the batcher. If the buffer is full, an error is returned.
func (b *Batcher[T]) Push(item T) error {
	if b.isStopped.Load() {
		return ErrBatcherStopped
	}

	select {
	case b.buffer <- item:
		return nil
	default:
		return ErrBufferFull
	}
}

// CurrentBufferSize returns the current amount of items in the buffer.
//
// Note that the buffer is not the amount of items pending to be flushed, it doesn't include items currently being
// flushed or being grouped into the next batch.
//
// You can use this to monitor the buffer size, when the buffer fills up you won't be able to push additional items.
func (b *Batcher[T]) CurrentBufferSize() int {
	return len(b.buffer)
}
