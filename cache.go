package gokache

import (
	"encoding/gob"
	"os"
	"sync"
	"time"
)

// CacheEntry represents a single item stored within the cache, encapsulating 
// both the actual data and its specific time-to-live.
type CacheEntry struct {
	// Value is the actual data stored in the cache. Being of type `any`, it can hold any Go data type.
	// NOTE: If you plan to use SaveToDisk/LoadFromDisk, any custom struct types stored here 
	// MUST be registered using gob.Register(MyStruct{}) in your application's init() function.
	Value any

	// Expiration represents the exact date and time when this entry becomes stale.
	// A zero-value time.Time indicates that the entry never expires.
	Expiration time.Time
}

// shard represents a fractional segment of the overall cache.
// Sharding minimizes thread contention by distributing locks across multiple map instances
// rather than locking the entire cache during concurrent read/write operations.
type shard struct {
	// items is the underlying map storing the cached key-value pairs for this specific shard.
	items map[string]CacheEntry

	// mu is a Read-Write mutex that safely controls concurrent access to the items map.
	// It allows multiple simultaneous readers but strictly one writer.
	mu sync.RWMutex
}

// Cache is a scalable, highly-concurrent in-memory key-value store with TTL (Time-To-Live) support.
// It relies on a sharded map architecture to reduce lock contention and includes an automated 
// background garbage collector for expired keys.
type Cache struct {
	// shards is a fixed-size array of map segments. Keys are hashed to determine their corresponding shard.
	shards []*shard

	// ttl defines the default expiration duration for newly added items if no specific TTL is provided.
	ttl time.Duration

	// cleanupInterval dictates how frequently the background garbage collector scans for and deletes expired items.
	cleanupInterval time.Duration

	// closeCh is a channel used to send a termination signal to the background cleanup goroutine.
	closeCh chan struct{}

	// closeOnce ensures that the closeCh channel is closed exactly once to prevent runtime panics.
	closeOnce sync.Once
}

// NewCache instantiates and returns a new Cache instance with a fixed number of shards (256).
// ttl determines the default time-to-live for entries (<= 0 implies no expiration).
// cleanupInterval sets the frequency of the background eviction process (0 disables the background process).
func NewCache(ttl time.Duration, cleanupInterval time.Duration) *Cache {
	const numShards = 256
	shards := make([]*shard, numShards)
	for i := range shards {
		shards[i] = &shard{items: make(map[string]CacheEntry)}
	}

	c := &Cache{
		shards:          shards,
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	// Start the cleanup goroutine if cleanupInterval is positive
	if cleanupInterval > 0 {
		go c.cleanupExpiredItems()
	}

	return c
}

// getShard calculates the FNV-1a hash of the provided string key inline and returns the specific 
// shard responsible for storing or retrieving this key. 
// This custom implementation ensures zero heap allocations compared to hash/fnv.
func (c *Cache) getShard(key string) *shard {
	var hash uint32 = 2166136261 // FNV offset basis
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619 // FNV prime
	}
	return c.shards[hash%uint32(len(c.shards))]
}

// Set adds a new key-value pair to the cache, applying the Cache instance's default TTL.
func (c *Cache) Set(key string, value any) {
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL adds a new key-value pair to the cache with a custom, item-specific TTL.
func (c *Cache) SetWithTTL(key string, value any, ttl time.Duration) {
	shard := c.getShard(key)
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	shard.mu.Lock()
	shard.items[key] = CacheEntry{Value: value, Expiration: exp}
	shard.mu.Unlock()
}

// Get attempts to retrieve an item from the cache using its key.
// It returns the value and a boolean indicating success. If the item is found but has expired, 
// it proactively deletes the item and returns false.
func (c *Cache) Get(key string) (any, bool) {
	shard := c.getShard(key)

	shard.mu.RLock()
	entry, ok := shard.items[key]
	shard.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Passive expiration check: if the entry is expired, explicitly delete it and return false.
	if !entry.Expiration.IsZero() && time.Now().After(entry.Expiration) {
		shard.mu.Lock()
		// Double-check under write lock to ensure another goroutine hasn't updated the entry
		if entry, ok = shard.items[key]; ok && !entry.Expiration.IsZero() && time.Now().After(entry.Expiration) {
			delete(shard.items, key)
			shard.mu.Unlock()
			return nil, false
		}
		shard.mu.Unlock()
	}

	return entry.Value, true
}

// Delete explicitly removes an item from the cache using its key.
func (c *Cache) Delete(key string) {
	shard := c.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

// Size aggregates and returns the total number of items currently stored across all shards.
func (c *Cache) Size() int {
	size := 0
	for _, s := range c.shards {
		s.mu.RLock()
		size += len(s.items)
		s.mu.RUnlock()
	}
	return size
}

// Close gracefully shuts down the background cleanup goroutine.
// It uses sync.Once to ensure it can be safely called multiple times without panicking.
// If the background cleanup was disabled, it immediately purges expired items synchronously.
func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		close(c.closeCh)

		// Signal the cleanup goroutine to stop; if no goroutine, clean up directly
		if c.cleanupInterval <= 0 {
			c.deleteExpiredItems()
		}
	})
}

// cleanupExpiredItems is a blocking background worker that periodically scans for and purges 
// expired items based on the cleanupInterval.
func (c *Cache) cleanupExpiredItems() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpiredItems()
		case <-c.closeCh:
			return
		}
	}
}

// deleteExpiredItems iterates through all shards and deletes any keys that have passed their expiration time.
func (c *Cache) deleteExpiredItems() {
	now := time.Now()
	for _, s := range c.shards {
		s.mu.Lock()
		for key, entry := range s.items {
			if !entry.Expiration.IsZero() && now.After(entry.Expiration) {
				delete(s.items, key)
			}
		}
		s.mu.Unlock()
	}
}

// SaveToDisk serializes the entire cache state and writes it to the specified file path using the gob encoder.
// It writes to a temporary file first and performs an atomic rename to prevent file corruption in case of a crash.
// Note: Custom structs stored as `any` must be registered via `gob.Register()` beforehand.
func (c *Cache) SaveToDisk(path string) error {
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(file)
	for _, s := range c.shards {
		s.mu.RLock()
		err := encoder.Encode(s.items)
		s.mu.RUnlock()
		
		if err != nil {
			file.Close()
			os.Remove(tempPath)
			return err
		}
	}

	// Ensure the file is successfully flushed and closed before renaming
	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Atomically replace the old cache file with the new one
	return os.Rename(tempPath, path)
}

// LoadFromDisk reads and deserializes the cache state from the specified file path using the gob decoder.
// Note: Custom structs stored as `any` must be registered via `gob.Register()` beforehand.
func (c *Cache) LoadFromDisk(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for _, s := range c.shards {
		s.mu.Lock()
		err := decoder.Decode(&s.items)
		s.mu.Unlock()
		
		if err != nil {
			return err
		}
	}
	return nil
}
