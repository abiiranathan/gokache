# gokache

**gokache** is a high-performance, sharded, in-memory cache written in Go with TTL (time-to-live) support, persistence, and optional periodic cleanup of expired items. It’s suitable for applications needing fast, concurrent caching with minimal lock contention.

---

## Features

- 🚀 **Sharded for concurrency** — 256 shards reduce lock contention for parallel reads and writes.
- ⏱️ **TTL support** — Store items with expiration times.
- ♻️ **Background cleanup** — Periodically remove expired items.
- 💾 **Persistence** — Save and load cache from disk using `gob`.
- 🧹 **Graceful shutdown** — Cleanup expired items or stop background goroutines safely.


## Installation

```bash
go get github.com/yourusername/gokache
```


## Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/yourusername/gokache"
)

func main() {
	cache := gokache.NewCache(5*time.Minute, 1*time.Minute)
	defer cache.Close()

	cache.Set("foo", "bar")

	val, ok := cache.Get("foo")
	if ok {
		fmt.Println("Got value:", val)
	}

	// Save to disk
	if err := cache.SaveToDisk("cache.snapshot"); err != nil {
		fmt.Println("Save error:", err)
	}

	// Load from disk
	if err := cache.LoadFromDisk("cache.snapshot"); err != nil {
		fmt.Println("Load error:", err)
	}
}
```

---

## API

### `NewCache(ttl time.Duration, cleanupInterval time.Duration) *Cache`

Creates a new cache instance.

* `ttl`: Default time-to-live for items. `<= 0` means no expiration.
* `cleanupInterval`: Interval for cleaning expired items. `0` disables cleanup goroutine.

### `Set(key string, value any)`

Stores an item with the default TTL.

### `SetWithTTL(key string, value any, ttl time.Duration)`

Stores an item with a custom TTL.

### `Get(key string) (any, bool)`

Retrieves a cached item if not expired.

### `Delete(key string)`

Removes an item from the cache.

### `Size() int`

Returns the total number of items.

### `Close()`

Cleans up resources and stops the background cleanup goroutine.

### `SaveToDisk(path string) error`

Saves the cache to a file using `gob`.

### `LoadFromDisk(path string) error`

Loads cache data from a file.

---

## Snapshotting & AOF Rotation

For long-running applications, you can periodically snapshot the cache:

```go
go func() {
    ticker := time.NewTicker(1 * time.Hour)
    for range ticker.C {
        cache.SaveToDisk("cache.snapshot")
        // Implement AOF rotation logic if needed
    }
}()
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.

---

## Contributing

Contributions are welcome! Please open an issue or PR.

```

