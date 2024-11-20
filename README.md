# 🦇 batchman

`batchman` provides an in-memory batcher that calls a function with a batch of items when the batch is full or a timeout is reached.

It is useful for batching requests to a service to reduce the total number of calls made.

For example you may only want to send events to your analytics service in batches of 1000 items or after 10 seconds, whichever comes first.

Batchman provides the controls you need to implement graceful shutdown where you don't lose any data.

## Features

* Supports batching up to a maximum number of items or a maximum time duration.
* Strongly typed with generics.
* Thread-safe.
* Context-aware.
* Buffered to avoid blocking the caller, with a configurable buffer size.
* Support for graceful shutdown without losing any items.

## Usage

You define a function that will be called with a batch of items. It will never be called with an empty slice.
The function will be called in a non-overlapping manner, i.e. the next call will only be made after the previous one has returned.

```go
func Flush(ctx context.Context, items []MyItemType) {
	// Handle the items.
}
```

You then use the builder pattern with `batchman.New[MyItemType]()` and configure it with `MaxSize`, `MaxDelay` and `BufferSize`.

```go
init := batchman.New[MyItemType]().MaxSize(2_000).MaxDelay(10 * time.Second)
```

Finally, you start the batcher with `Start` and push items to it with `Push`.

```go
batcher, err := init.Start(ctx, Flush)
if err != nil {
	panic(err)
}

err = batcher.Push(MyItemType{Some: "value"})
if err != nil {
	panic(err)
}
```

When you are done (or shutting down your program), cancel the context passed to `Start` to stop the batcher. The `Done` channel will be closed when the batcher has finished flushing the remaining data.

```go
cancel()

<-batcher.Done()
```


### Full example

```go
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
	ctx, cancel := context.WithCancel(context.Background())

	// Define a function that will be called with a batch of items.
	flush := func(_ context.Context, items []MyItemType) {
		fmt.Println("Flushing a batch of", len(items), "items")
		time.Sleep(1 * time.Second)
	}

	init := batchman.New[MyItemType]().MaxSize(2000).MaxDelay(10 * time.Second)

	// Start the batcher, it will only error immediately or not at all. This is a non-blocking call.
	batcher, err := init.Start(ctx, flush)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5000; i++ {
		// Add items to the batcher, this is a non-blocking call.
		err = batcher.Push(MyItemType{ID: i, Name: "Some Name"})
		if err != nil {
			// This errors if the batcher has been stopped (by cancelling the context), or if the
			// buffer is full and the batcher is unable to accept more items.
			panic(err)
		}
	}

	cancel()

	// Wait for the batcher to finish flushing.
	<-batcher.Done()
}
```

For more examples, see the [example](example) directory.

# Tips

## Graceful Shutdown

To gracefully shutdown the batcher, cancel the context passed to `Start`. This will cause the batcher to flush any remaining items and return. The `Done` channel will be closed when the batcher has finished flushing.

The context that is passed to the flush function is cancelled when the batcher is stopped, you can use this to stop any long-running flush operations. Note that any remaining data in the buffer will have the flush function called with a cancelled context, so you should decide how to handle this in your implementation.

You may want to use `context.WithoutCancel` to create a new context that is not cancelled when the batcher is stopped. This can be useful if you want to give the last remaining batches a chance to finish when shutting down your program.

```go
func MyFlushFunc(ctx context.Context, items []MyItemType) {
    ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5 * time.Second)
    defer cancel()
    
    // Handle the items.
}
```

Alternatively you can also use a `select { case <- ctx.Done() ... }` to check if the context has since been cancelled.

## Timeout on shutdown

While unlikely, it is possible that the buffer is backed up and there are multiple batches that need to be flushed on shutdown.

You may want to wait a maximum time for `batcher.Done()` to close. 

```go
select {
case <-batcher.Done():
    // Done flushing all items.
case <-time.After(20 * time.Second):
    // Timed out waiting for the batcher to finish flushing.
}
```

## License

[MIT](LICENSE)
