package gokache

import (
	"encoding/gob"
	"hash/fnv"
	"os"
	"sync"
	"time"
)

// CacheEntry represents a cached item with its value and expiration time.
// A zero Expiration means the entry does not expire.
type CacheEntry struct {
	Value      any       // The value stored in the cache
	Expiration time.Time // The expiration time of the entry. Zero means no expiration
}

// shard represents a single shard of the cache.
// Each shard is a map of string keys to CacheEntry values.
// It uses a read-write mutex to allow concurrent access.
type shard struct {
	items map[string]CacheEntry // Map of keys to CacheEntry values
	mu    sync.RWMutex          // Read-write mutex for concurrent access
}

// Cache is a scalable in-memory cache with TTL support.
// It uses sharding to improve performance and reduce contention.
type Cache struct {
	shards          []*shard      // Array of shards for sharding
	ttl             time.Duration // Default time-to-live for entries
	cleanupInterval time.Duration // Interval for background cleanup of expired items
	closeCh         chan struct{} // Channel to signal cleanup goroutine to stop
}

// NewCache creates a new Cache instance with configurable sharding.
// ttl: Default time-to-live for entries (<=0 means no expiration)
// cleanupInterval: Interval for background cleanup (0 disables cleanup)
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

func (c *Cache) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.shards[h.Sum32()%uint32(len(c.shards))]
}

// Set adds a value with the default TTL.
func (c *Cache) Set(key string, value any) {
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL adds a value with custom TTL.
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

// Get retrieves an item if available and not expired.
func (c *Cache) Get(key string) (any, bool) {
	shard := c.getShard(key)

	shard.mu.RLock()
	entry, ok := shard.items[key]
	shard.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if !entry.Expiration.IsZero() && time.Now().After(entry.Expiration) {
		shard.mu.Lock()
		defer shard.mu.Unlock()
		// Double-check under write lock
		if entry, ok = shard.items[key]; ok &&
			!entry.Expiration.IsZero() && time.Now().After(entry.Expiration) {
			delete(shard.items, key)
			return nil, false
		}
	}

	return entry.Value, true
}

// Delete removes an item from the cache.
func (c *Cache) Delete(key string) {
	shard := c.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

// Size returns the total number of items in the cache.
func (c *Cache) Size() int {
	size := 0
	for _, s := range c.shards {
		s.mu.RLock()
		size += len(s.items)
		s.mu.RUnlock()
	}
	return size
}

// Close stops the background cleanup goroutine.
func (c *Cache) Close() {
	close(c.closeCh)

	// Signal the cleanup goroutine to stop
	if c.cleanupInterval <= 0 {
		// no go routine to stop but we need to clean up
		c.deleteExpiredItems()
	}
}

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

// SaveToDisk saves the cache to a file using gob encoding.
func (c *Cache) SaveToDisk(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	for _, s := range c.shards {
		s.mu.RLock()
		if err := encoder.Encode(s.items); err != nil {
			s.mu.RUnlock()
			return err
		}
		s.mu.RUnlock()
	}
	return nil

}

// LoadFromDisk loads the cache from a file using gob decoding.
func (c *Cache) LoadFromDisk(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for _, s := range c.shards {
		s.mu.Lock()
		if err := decoder.Decode(&s.items); err != nil {
			s.mu.Unlock()
			return err
		}
		s.mu.Unlock()
	}
	return nil
}
