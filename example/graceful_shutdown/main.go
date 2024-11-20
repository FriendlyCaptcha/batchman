// Package main provides example binaries that demonstrates how the batcher behaves.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/friendlycaptcha/batchman"
)

const (
	withGracefulShutdown = true
	maxBatchSize         = 4
	maxBatchDelay        = time.Second * 5
	bufferSize           = 100

	gracefulShutdownTimeout = time.Second * 4
	flushDelay              = time.Second * 2

	topLevelWaitTimeForShutdown = time.Second * 8
)

func flushFunc(ctx context.Context, values []string) {
	if withGracefulShutdown {
		gctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gracefulShutdownTimeout)
		defer cancel()
		ctx = gctx
	} else {
		sctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ctx = sctx
	}

	if ctx.Err() != nil {
		fmt.Printf("Context cancelled before flushing %d items: %v\n", len(values), values)
		return
	}
	fmt.Printf("\nSTARTED FLUSHING %d items: %v\n", len(values), values)

	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)

		t := time.After(flushDelay)

		select {
		case <-ctx.Done():
			fmt.Printf("Context cancelled while flushing %d items: %v\n", len(values), values)
			return
		case <-t:
			fmt.Printf("\nFINISH FLUSHING %d items: %v\n", len(values), values)
		}
	}()

	<-doneChan
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	b := batchman.New[string]().MaxSize(maxBatchSize).BufferSize(bufferSize).MaxDelay(maxBatchDelay)

	batcher, err := b.Start(ctx, flushFunc)
	if err != nil {
		fmt.Printf("Failed to start batcher: %v\n", err)
		os.Exit(1)
	}

	fmt.Print("Started, type words and press ENTER to add items to the batcher.\n Type exit to stop the batcher.\n")

	go func() {
		// Listen for keyboard input SPACE to add items to the batcher
		for {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				item := scanner.Text()

				if item == "exit" {
					cancel()
					continue
				}

				err := batcher.Push(item)
				if err != nil {
					fmt.Printf("Failed to add item %s to batcher: %v\n", item, err)
				}
			}
		}
	}()

	<-ctx.Done()
	fmt.Println("Context cancelled, waiting for batcher to stop.")

	select {
	case <-batcher.Done():
		fmt.Println("Batcher stopped.")
	case <-time.After(topLevelWaitTimeForShutdown):
		fmt.Println("Timeout waiting for batcher to stop.")
	}

	fmt.Println("FIN.")
}
