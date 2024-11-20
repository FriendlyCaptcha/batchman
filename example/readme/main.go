// Package readme contains the code snippet from the README.md file.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/friendlycaptcha/batchman"
)

// MyItemType is a type that will be batched.
type MyItemType struct {
	ID   int
	Name string
}

func main() {
	// Define a function that will be called with a batch of items.
	// It will be called in a non-overlapping manner, i.e. the next call will only be made after the previous
	// one has returned.
	flush := func(_ context.Context, items []MyItemType) {
		fmt.Println("Flushing a batch of", len(items), "items")
		time.Sleep(1 * time.Second)
	}

	// Create a new batcher that will create batches with up to 2000 items, or after at most a 10 second delay.
	// The default buffer size is 10_000 items.
	init := batchman.New[MyItemType]().MaxSize(2000).MaxDelay(10 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())

	// Start the batcher, it will only error immediately or not at all. This is a non-blocking call.
	batcher, err := init.Start(ctx, flush)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5000; i++ {
		// Add items to the batcher, this is a non-blocking call.
		err = batcher.Push(MyItemType{ID: i, Name: "Some Name"})
		if err != nil {
			// This only errors if the batcher has been stopped (by cancelling the context), or if the
			// buffer is full and the batcher is unable to accept more items.
			panic(err)
		}
	}

	// Stop the batcher by cancelling the context, this will flush any remaining items.
	cancel()

	// Wait for the batcher to finish flushing.
	<-batcher.Done()
}
