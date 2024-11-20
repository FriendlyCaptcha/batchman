package batchman

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidMaxSize is returned when the max size is invalid.
	ErrInvalidMaxSize = errors.New("max size must be greater than 0")
	// ErrInvalidBufferSize is returned when the buffer size is invalid.
	ErrInvalidBufferSize = errors.New("buffer size must be greater than 0")
	// ErrInvalidMaxDelay is returned when the max delay is invalid.
	ErrInvalidMaxDelay = errors.New("max delay must be greater than 0")

	// ErrBufferFull is returned when the buffer is full. This means the item was not added.
	ErrBufferFull = errors.New("buffer is full")

	// ErrBatcherStopped is returned when the batcher has been stopped (through a context cancellation).
	// No more items can be added.
	ErrBatcherStopped = errors.New("batcher has been stopped")
)

// Builder is a builder for creating a new Batcher.
type Builder[T any] struct {
	maxSize    int
	maxDelay   time.Duration
	bufferSize int
}

// New creates a new Builder with default values.
// The default values are a batch size of 1,000, a maximum delay of 10 seconds, and a buffer size of 10,000 items.
func New[T any]() *Builder[T] {
	return &Builder[T]{
		maxSize:    1_000,
		maxDelay:   time.Second * 10,
		bufferSize: 10_000,
	}
}

// MaxSize sets the maximum number of items to batch together.
func (b *Builder[T]) MaxSize(maxSize int) *Builder[T] {
	b.maxSize = maxSize
	return b
}

// MaxDelay sets the maximum delay before flushing the batch.
func (b *Builder[T]) MaxDelay(maxDelay time.Duration) *Builder[T] {
	b.maxDelay = maxDelay
	return b
}

// BufferSize sets the buffer size for the batcher.
func (b *Builder[T]) BufferSize(bufferSize int) *Builder[T] {
	b.bufferSize = bufferSize
	return b
}

// Start a new Batcher with the configured values. This returns an error immediately if the configuration is invalid.
// If the context is cancelled, the Batcher will stop and flush any remaining items.
func (b *Builder[T]) Start(ctx context.Context, flush func(ctx context.Context, values []T)) (*Batcher[T], error) {
	if b.maxSize <= 0 {
		return nil, ErrInvalidMaxSize
	}
	if b.bufferSize <= 0 {
		return nil, ErrInvalidBufferSize
	}
	if b.maxDelay <= 0 {
		return nil, ErrInvalidMaxDelay
	}

	batcher := &Batcher[T]{
		buffer:  make(chan T, b.bufferSize),
		stopped: make(chan struct{}),
	}

	go batcher.start(ctx, b.maxDelay, b.maxSize, flush)
	return batcher, nil
}
