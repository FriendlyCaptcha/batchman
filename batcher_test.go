package batchman

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/friendlycaptcha/batchman/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startWithMockFlusher[T any](ctx context.Context, t *testing.T, b *Builder[T]) (*Batcher[T], *mock.Flusher[T]) {
	t.Helper()
	flusher := &mock.Flusher[T]{}
	batcher, err := b.Start(ctx, flusher.Flush)
	require.NoError(t, err)
	return batcher, flusher
}

type doneWatcher struct {
	sync.Mutex
	recv bool
}

func (s *doneWatcher) didReceiveDone() bool {
	s.Lock()
	defer s.Unlock()
	return s.recv
}

func watchDone(b *Batcher[int]) *doneWatcher {
	s := &doneWatcher{}
	go func() {
		<-b.Done()
		s.Lock()
		s.recv = true
		s.Unlock()
	}()
	return s
}

func TestBatcherBuffer(t *testing.T) {
	t.Parallel()

	t.Run("buffer full rejects new additions", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b, f := startWithMockFlusher(ctx, t, (New[int]().MaxSize(5).BufferSize(10)))
		f.Block()

		for i := 0; i < 5; i++ {
			assert.NoError(t, b.Push(i))
		}
		<-f.WaitForTotalStarted(1)

		for i := 0; i < 10; i++ {
			assert.NoError(t, b.Push(i), "expected no error adding item %d", i)
		}
		assert.Equal(t, b.CurrentBufferSize(), 10)
		assert.ErrorIs(t, b.Push(15), ErrBufferFull)

		f.Unblock()
		<-f.WaitForTotalFinished(3)

		// We can add more items now that the buffer has been flushed.
		assert.NoError(t, b.Push(15))
	})
}

func TestBatcherMaxItems(t *testing.T) {
	t.Parallel()

	t.Run("flushes at max items", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		batcher, flusher := startWithMockFlusher(ctx, t, (New[int]().MaxSize(3).MaxDelay(time.Millisecond * 10)))

		for i := 0; i < 10; i++ {
			assert.NoError(t, batcher.Push(i))
		}

		<-flusher.WaitForTotalFinished(4)

		assert.Equal(t, [][]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9}}, flusher.BatchesFlushed())
	})
}

func TestBatcherMaxDelay(t *testing.T) {
	t.Parallel()

	// For tests where the context doesn't matter we use this.
	ctx := context.Background()

	t.Run("flushes at max delay", func(t *testing.T) {
		t.Parallel()

		batcher, flusher := startWithMockFlusher(ctx, t, (New[int]().MaxDelay(time.Millisecond * 5)))

		for i := 0; i < 3; i++ {
			require.NoError(t, batcher.Push(i))
		}

		<-flusher.WaitForTotalFinished(1)

		assert.Equal(t, [][]int{{0, 1, 2}}, flusher.BatchesFlushed())
	})

	t.Run("waits the full delay before flushing", func(t *testing.T) {
		t.Parallel()
		batcher, flusher := startWithMockFlusher(ctx, t, (New[int]().MaxDelay(time.Millisecond * 20)))

		time.Sleep(time.Millisecond * 10)

		insertTime := time.Now()

		require.NoError(t, batcher.Push(0))
		<-flusher.WaitForTotalFinished(1)

		assert.Greater(t, time.Since(insertTime), time.Millisecond*15)
	})

	t.Run("Does not fire an additional time if items are added after flush", func(t *testing.T) {
		t.Parallel()
		batcher, flusher := startWithMockFlusher(ctx, t, (New[int]().MaxDelay(time.Millisecond * 10)))

		for i := 0; i < 3; i++ {
			require.NoError(t, batcher.Push(i))
		}

		time.Sleep(time.Millisecond * 5)

		for i := 3; i < 6; i++ {
			require.NoError(t, batcher.Push(i))
		}

		time.Sleep(time.Millisecond * 10)

		<-flusher.WaitForTotalFinished(1)

		assert.Equal(t, [][]int{{0, 1, 2, 3, 4, 5}}, flusher.BatchesFlushed())

		time.Sleep(time.Millisecond * 5)

		for i := 6; i < 9; i++ {
			require.NoError(t, batcher.Push(i))
		}

		time.Sleep(time.Millisecond * 8)

		// We should not have flushed again.
		assert.Equal(t, [][]int{{0, 1, 2, 3, 4, 5}}, flusher.BatchesFlushed())
	})
}

func TestBatcherCancellation(t *testing.T) {
	t.Parallel()

	t.Run("flushes when context is cancelled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())

		batcher, flusher := startWithMockFlusher(ctx, t, New[int]().MaxSize(3))

		for i := 0; i < 5; i++ {
			require.NoError(t, batcher.Push(i))
		}

		// Unfortunately this is the only way to give the batcher time to read the items..
		time.Sleep(time.Millisecond * 5)

		cancel()

		<-flusher.WaitForTotalStarted(2)
		time.Sleep(time.Millisecond * 10)

		assert.True(t, flusher.WasInterrupted())
		assert.Equal(t, [][]int{{0, 1, 2}}, flusher.BatchesFlushed())
	})

	t.Run("also calls flush on the remaining buffer in correct batch size when context is cancelled",
		func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b, f := startWithMockFlusher(ctx, t, (New[int]().MaxSize(5).BufferSize(8)))
			f.Block()

			for i := 0; i < 5; i++ {
				assert.NoError(t, b.Push(i))
			}
			<-f.WaitForTotalStarted(1)

			for i := 0; i < 8; i++ {
				assert.NoError(t, b.Push(i), "expected no error adding item %d", i)
			}
			assert.ErrorIs(t, b.Push(123), ErrBufferFull)

			// `Done` channel should only close after the last flush call has happened, even if cancelled.
			stopWatcher := watchDone(b)
			assert.False(t, stopWatcher.didReceiveDone())

			cancel()
			f.Unblock()

			// If this test passes we can uncomment
			<-f.WaitForTotalStarted(3)

			time.Sleep(time.Millisecond * 5)

			assert.True(t, stopWatcher.didReceiveDone())

			assert.True(t, f.WasInterrupted())
			assert.Equal(t, 3, f.StartCount()) // Three flushes, but no finishes as the ctx is cancelled.
			assert.Equal(t, 0, f.FinishCount())
			assert.Len(t, f.BatchesFlushed(), 0)
			assert.Equal(t, 0, b.CurrentBufferSize())
		},
	)

	t.Run("calls flush on the remaining buffer when context is cancelled, with graceful flush delay",
		func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b, f := startWithMockFlusher(ctx, t, (New[int]().MaxSize(5).BufferSize(8)))
			f.SetIgnoreContextCancellation(time.Second)
			f.Block()

			for i := 0; i < 5; i++ {
				assert.NoError(t, b.Push(i))
			}
			<-f.WaitForTotalStarted(1)

			for i := 5; i < 13; i++ {
				assert.NoError(t, b.Push(i), "expected no error adding item %d", i)
			}
			assert.ErrorIs(t, b.Push(123), ErrBufferFull)

			// `Done` channel should only close after the last flush call has happened, even if cancelled.
			// Despite the sleeping here, the `Done` channel should not close as the flushers are not finished.
			stopWatcher := watchDone(b)

			cancel()

			time.Sleep(time.Millisecond * 10)

			assert.False(t, stopWatcher.didReceiveDone())

			f.Unblock()

			// If this test passes we can uncomment
			<-f.WaitForTotalFinished(3)
			time.Sleep(time.Millisecond * 5)

			assert.True(t, stopWatcher.didReceiveDone())

			assert.False(t, f.WasInterrupted()) // Not interrupted because the flush give itself another second.
			assert.Equal(t, 3, f.StartCount())
			assert.Equal(t, 3, f.FinishCount())

			assert.Equal(t, [][]int{{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}, {10, 11, 12}}, f.BatchesFlushed())
		},
	)

	t.Run("does not accept new items after cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())

		batcher, _ := startWithMockFlusher(ctx, t, New[int]())
		cancel()

		time.Sleep(time.Millisecond * 5)

		assert.ErrorIs(t, batcher.Push(0), ErrBatcherStopped)
		assert.Equal(t, 0, batcher.CurrentBufferSize())
	})

	t.Run("no empty flush after cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())

		_, flusher := startWithMockFlusher(ctx, t, New[int]())

		cancel()

		time.Sleep(time.Millisecond * 5)

		assert.Equal(t, 0, flusher.StartCount())
	})
}
