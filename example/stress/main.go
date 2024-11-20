// Package main provides a stress test for the batcher package.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/friendlycaptcha/batchman"
	"golang.org/x/exp/rand"
)

func main() {
	for i := 0; i < 100; i++ {
		writers := rand.Intn(1000) + 1
		pushPerWriter := rand.Intn(1000) + 1
		maxSize := rand.Intn(1000) + 1
		maxDelay := time.Duration(rand.Intn(1000)) * time.Microsecond
		test(writers, pushPerWriter, maxSize, maxDelay)
	}
}

func test(writers int, pushPerWriter int, maxSize int, maxDelay time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	expectTotalFlush := writers * pushPerWriter
	totalFlushed := 0

	fmt.Printf(
		"📋 Test with %d writers, %d pushPerWriter, %d maxSize, maxDelay %s\n",
		writers,
		pushPerWriter,
		maxSize,
		maxDelay.String(),
	)
	init := batchman.New[bool]().MaxSize(maxSize).MaxDelay(maxDelay).BufferSize(expectTotalFlush)
	b, err := init.Start(ctx, func(_ context.Context, items []bool) {
		totalFlushed += len(items)
	})
	if err != nil {
		panic(err)
	}

	// Let all the threads finish.
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < pushPerWriter; j++ {
				e := b.Push(true)
				// Shouldn't happen, the buffer should be large enough.
				if e != nil {
					panic(e)
				}
				time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
			}
			wg.Done()
		}()
	}
	fmt.Printf(" - ⏱️ ... Waiting for Writers to finish\n")
	wg.Wait()

	fmt.Printf(" - ⏱️ ... Cancelling context \n")
	// Cancel the context to stop the batcher.
	cancel()

	fmt.Printf(" - ⏱️ ... Waiting for batcher to finish\n")
	// Wait for the batcher to finish.
	<-b.Done()

	if totalFlushed != expectTotalFlush {
		fmt.Printf(" - ❌ totalFlushed (%d) != expectTotalFlush (%d)\n", totalFlushed, expectTotalFlush)
	} else {
		fmt.Printf(" - ✅ Test good\n")
	}
}
