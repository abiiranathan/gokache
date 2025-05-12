package gokache

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSaveAndLoadCache(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	cacheFile := filepath.Join(tempDir, "cache.gob")

	c := NewCache(10*time.Minute, 0)
	c.Set("key1", "value1")
	c.Set("key2", 42)
	c.SetWithTTL("key3", 3.14, time.Minute)

	// Test SaveToDisk
	if err := c.SaveToDisk(cacheFile); err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(cacheFile); errors.Is(err, os.ErrNotExist) {
		t.Fatal("Cache file was not created")
	}

	defer os.Remove(cacheFile) // Clean up after test

	// Create new cache instance and load
	c2 := NewCache(10*time.Minute, 0)
	if err := c2.LoadFromDisk(cacheFile); err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}

	// Verify loaded data
	tests := []struct {
		key    string
		value  any
		exists bool
	}{
		{"key1", "value1", true},
		{"key2", 42, true},
		{"key3", 3.14, true},
		{"nonexistent", nil, false},
	}

	for _, tt := range tests {
		val, ok := c2.Get(tt.key)
		if ok != tt.exists {
			t.Errorf("Key %q existence mismatch: got %v, want %v", tt.key, ok, tt.exists)
		}
		if ok && !reflect.DeepEqual(val, tt.value) {
			t.Errorf("Key %q value mismatch: got %v, want %v", tt.key, val, tt.value)
		}
	}
}

func TestSaveLoadEmptyCache(t *testing.T) {
	tempDir := t.TempDir()
	cacheFile := filepath.Join(tempDir, "empty_cache.gob")

	c := NewCache(time.Minute, 0)

	// Save empty cache
	if err := c.SaveToDisk(cacheFile); err != nil {
		t.Fatalf("SaveToDisk failed for empty cache: %v", err)
	}
	defer os.Remove(cacheFile) // Clean up after test

	// Load into new cache
	c2 := NewCache(time.Minute, 0)
	if err := c2.LoadFromDisk(cacheFile); err != nil {
		t.Fatalf("LoadFromDisk failed for empty cache: %v", err)
	}

	if size := c2.Size(); size != 0 {
		t.Errorf("Expected empty cache after load, got size %d", size)
	}
}

func TestSaveLoadConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	cacheFile := filepath.Join(tempDir, "concurrent_cache.gob")

	c := NewCache(time.Minute, 0)
	var wg sync.WaitGroup

	// Concurrent writers during save
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Set(string(rune(i)), i)
		}(i)
	}

	// Save while concurrent writes are happening
	if err := c.SaveToDisk(cacheFile); err != nil {
		t.Fatalf("SaveToDisk failed with concurrent access: %v", err)
	}
	defer os.Remove(cacheFile) // Clean up after test

	wg.Wait()

	// Load and verify
	c2 := NewCache(time.Minute, 0)
	if err := c2.LoadFromDisk(cacheFile); err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}

	// Verify at least some data was saved
	if c2.Size() == 0 {
		t.Error("No data was saved during concurrent access")
	}
}

func TestSaveToDiskErrors(t *testing.T) {
	c := NewCache(time.Minute, 0)

	// Test invalid path
	invalidPath := filepath.Join(string(os.PathSeparator), "nonexistent", "dir", "cache.gob")
	err := c.SaveToDisk(invalidPath)
	if err == nil {
		t.Error("Expected error for invalid path, got nil")
	}

	// Test permission denied (Unix-like systems)
	if os.Geteuid() != 0 { // Skip if running as root
		readOnlyDir := filepath.Join(t.TempDir(), "readonly")
		if err := os.Mkdir(readOnlyDir, 0444); err != nil {
			t.Fatalf("Failed to create read-only dir: %v", err)
		}
		readOnlyFile := filepath.Join(readOnlyDir, "cache.gob")
		err = c.SaveToDisk(readOnlyFile)
		if err == nil {
			t.Error("Expected permission denied error, got nil")
		}
	}
}

func TestLoadFromDiskErrors(t *testing.T) {
	c := NewCache(time.Minute, 0)

	// Test non-existent file
	err := c.LoadFromDisk("nonexistent_file.gob")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}

	// Test invalid file format
	tempDir := t.TempDir()
	invalidFile := filepath.Join(tempDir, "invalid.gob")
	if err := os.WriteFile(invalidFile, []byte("not a gob file"), 0644); err != nil {
		t.Fatalf("Failed to create invalid test file: %v", err)
	}

	err = c.LoadFromDisk(invalidFile)
	if err == nil {
		t.Error("Expected error for invalid gob data, got nil")
	}
}

func TestSaveLoadWithExpiration(t *testing.T) {
	tempDir := t.TempDir()
	cacheFile := filepath.Join(tempDir, "expiring_cache.gob")

	c := NewCache(time.Minute, 0)
	c.SetWithTTL("expiring", "value", time.Minute)
	c.Set("permanent", "value")

	if err := c.SaveToDisk(cacheFile); err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}
	defer os.Remove(cacheFile) // Clean up after test

	c2 := NewCache(time.Minute, 0)
	if err := c2.LoadFromDisk(cacheFile); err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}

	// Verify expiration times were preserved
	shard := c2.getShard("expiring")
	shard.mu.RLock()
	entry, ok := shard.items["expiring"]
	shard.mu.RUnlock()

	if !ok {
		t.Error("Expiring key not found after load")
	}
	if entry.Expiration.IsZero() {
		t.Error("Expiration time not preserved for expiring key")
	}
}
