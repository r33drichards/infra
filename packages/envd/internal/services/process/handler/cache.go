package handler

import (
	"sync"
	"time"
)

// CachedMultiplex wraps MultiplexedChannel and stores values in TimestampedList
type CachedMultiplex[T any] struct {
	forker          ForkSource[T]
	timestampedList *TimestampedList[T]
	mu              sync.RWMutex
}

type ForkSource[T any] interface {
	Fork() (chan T, func())
	Source() chan T
}

// NewCachedMultiplex creates a new CachedMultiplex
func NewCachedMultiplex[T any](f ForkSource[T]) *CachedMultiplex[T] {
	cm := &CachedMultiplex[T]{
		forker:          f,
		timestampedList: &TimestampedList[T]{},
	}

	go cm.start()

	return cm
}

// start reads from the source channel and stores values in TimestampedList
func (cm *CachedMultiplex[T]) start() {
	ch, cancel := cm.Fork()
	defer cancel()
	for v := range ch {
		cm.mu.Lock()
		cm.timestampedList.AddEntry(time.Now(), v)
		cm.mu.Unlock()
	}
}

// Fork creates a new consumer channel and returns it along with a cancel function
func (cm *CachedMultiplex[T]) Fork() (chan T, func()) {
	return cm.forker.Fork()
}

// Source returns the source channel
func (cm *CachedMultiplex[T]) Source() chan T {
	return cm.forker.Source()
}

// entriesAfter retrieves all entries that occur after the given timestamp
func (cm *CachedMultiplex[T]) entriesAfter(timestamp time.Time) []TimestampedData[T] {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.timestampedList.EntriesAfter(timestamp)
}

// populateEntriesAfter retrieves all entries that occur after the given timestamp and sends them to the provided channel
func (cm *CachedMultiplex[T]) PopulateEntriesAfter(timestamp time.Time) *CachedMultiplex[T] {

	entries := cm.entriesAfter(timestamp)
	for _, entry := range entries {
		cm.Source() <- entry.Data
	}
	return cm
}
