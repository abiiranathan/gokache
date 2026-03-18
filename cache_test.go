package gokache

import (
	"sync"
	"testing"
	"time"
)

func TestCacheBasicOperations(t *testing.T) {
	c := NewCache(10*time.Minute, 0) // No cleanup for this test

	// Test Set and Get
	c.Set("key1", "value1")
	if val, ok := c.Get("key1"); !ok || val != "value1" {
		t.Errorf("Get after Set failed, got %v, %v, want %v, %v", val, ok, "value1", true)
	}

	// Test Get non-existent key
	if val, ok := c.Get("nonexistent"); ok || val != nil {
		t.Errorf("Get non-existent key failed, got %v, %v, want %v, %v", val, ok, nil, false)
	}

	// Test Delete
	c.Delete("key1")
	if val, ok := c.Get("key1"); ok || val != nil {
		t.Errorf("Get after Delete failed, got %v, %v, want %v, %v", val, ok, nil, false)
	}

	// Test Size
	c.Set("key1", "value1")
	c.Set("key2", "value2")
	if size := c.Size(); size != 2 {
		t.Errorf("Size failed, got %d, want %d", size, 2)
	}
}

func TestCacheExpiration(t *testing.T) {
	// Test with very short TTL
	c := NewCache(100*time.Millisecond, 0)

	// Set value that should expire quickly
	c.Set("temp", "value")
	if val, ok := c.Get("temp"); !ok || val != "value" {
		t.Errorf("Get before expiration failed")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)
	if val, ok := c.Get("temp"); ok || val != nil {
		t.Errorf("Get after expiration failed, got %v, %v, want %v, %v", val, ok, nil, false)
	}

	// Test with custom TTL
	c.SetWithTTL("long", "value", time.Second)
	c.SetWithTTL("short", "value", 100*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	if _, ok := c.Get("short"); ok {
		t.Error("Short-lived item should have expired")
	}
	if _, ok := c.Get("long"); !ok {
		t.Error("Long-lived item should still exist")
	}
}

func TestCacheNoExpiration(t *testing.T) {
	c := NewCache(0, 0) // No expiration

	c.Set("perm", "value")
	time.Sleep(100 * time.Millisecond)
	if val, ok := c.Get("perm"); !ok || val != "value" {
		t.Errorf("Permanent item failed, got %v, %v, want %v, %v", val, ok, "value", true)
	}
}

func TestCacheCleanup(t *testing.T) {
	c := NewCache(100*time.Millisecond, 50*time.Millisecond)

	// Set items that will expire
	for i := range 10 {
		c.SetWithTTL("key"+string(rune('a'+i)), i, 50*time.Millisecond)
	}

	// Set one permanent item
	c.Set("perm", "value")

	initialSize := c.Size()
	if initialSize != 11 {
		t.Errorf("Initial size wrong, got %d, want %d", initialSize, 11)
	}

	// Wait for cleanup
	time.Sleep(200 * time.Millisecond)

	finalSize := c.Size()
	if finalSize != 0 {
		t.Errorf("Final size wrong after cleanup, got %d, want %d", finalSize, 0)
	}

	if _, ok := c.Get("perm"); ok {
		t.Errorf("Permanent item should also be deleted by default expiration")
	}

	c.Close() // Cleanup
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewCache(time.Minute, 0)
	var wg sync.WaitGroup

	// Concurrent writers
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Set(string(rune(i)), i)
		}(i)
	}

	// Concurrent readers
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Get(string(rune(i)))
		}(i)
	}

	// Concurrent deletes
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Delete(string(rune(i)))
		}(i)
	}

	wg.Wait()

	// Verify remaining items
	size := c.Size()
	if size < 50 || size > 100 {
		t.Errorf("Unexpected size after concurrent operations: %d", size)
	}
}

func TestCacheClose(t *testing.T) {
	c := NewCache(100*time.Millisecond, 50*time.Millisecond)

	// Set some items that would expire
	c.Set("temp", "value")

	// Close the cache
	c.Close()

	// Verify we can still access cache after close
	if val, ok := c.Get("temp"); !ok || val != "value" {
		t.Errorf("Cache access after Close failed")
	}

	// Verify passive expiration still works (Get should cleanup)
	if _, ok := c.Get("should_expire"); ok {
		t.Error("Expired item should be removed by Get() even after Close")
	}
}

func TestCacheSharding(t *testing.T) {
	c := NewCache(time.Minute, 0)

	// Fill the cache with enough items to test shard distribution
	for i := range 1000 {
		c.Set(string(rune(i)), i)
	}

	// Verify all items are retrievable
	for i := range 1000 {
		if val, ok := c.Get(string(rune(i))); !ok || val != i {
			t.Errorf("Sharded cache failed for key %d", i)
		}
	}
}

func BenchmarkCacheGet(b *testing.B) {
	c := NewCache(time.Minute, 0)
	c.Set("key", "value")

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.Get("key")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
}

func BenchmarkCacheSet(b *testing.B) {
	c := NewCache(time.Minute, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.Set("key", "value")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
}

func BenchmarkCacheConcurrent(b *testing.B) {
	c := NewCache(time.Minute, 0)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Set("key", "value")
			c.Get("key")
		}
	})
	ops := float64(b.N * 2) // Each iteration does 1 Set + 1 Get
	b.ReportMetric(ops/b.Elapsed().Seconds(), "ops/sec")
}
