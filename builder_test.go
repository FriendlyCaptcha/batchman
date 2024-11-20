package batchman

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalidBuilderArguments(t *testing.T) {
	t.Parallel()
	dummyFlush := func(_ context.Context, _ []int) {}

	_, err := New[int]().MaxSize(0).Start(context.Background(), dummyFlush)
	assert.ErrorIs(t, err, ErrInvalidMaxSize)

	_, err = New[int]().BufferSize(0).Start(context.Background(), dummyFlush)
	assert.ErrorIs(t, err, ErrInvalidBufferSize)

	_, err = New[int]().MaxDelay(0).Start(context.Background(), dummyFlush)
	assert.ErrorIs(t, err, ErrInvalidMaxDelay)
}
