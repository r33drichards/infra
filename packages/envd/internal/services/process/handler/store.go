package handler

import (
    "sort"
    "sync"
    "time"
)

type TimestampedData[T any] struct {
    Timestamp time.Time
    Data      T
}

type TimestampedList[T any] struct {
    mu      sync.RWMutex
    Entries []TimestampedData[T]
}

func (tl *TimestampedList[T]) AddEntry(timestamp time.Time, data T) {
    tl.mu.Lock()
    defer tl.mu.Unlock()
    
    entry := TimestampedData[T]{Timestamp: timestamp, Data: data}
    tl.Entries = append(tl.Entries, entry)
    sort.Slice(tl.Entries, func(i, j int) bool {
        return tl.Entries[i].Timestamp.Before(tl.Entries[j].Timestamp)
    })
}

func (tl *TimestampedList[T]) EntriesAfter(timestamp time.Time) []TimestampedData[T] {
    tl.mu.RLock()
    defer tl.mu.RUnlock()
    
    idx := sort.Search(len(tl.Entries), func(i int) bool {
        return tl.Entries[i].Timestamp.After(timestamp)
    })

    if idx < len(tl.Entries) {
        result := make([]TimestampedData[T], len(tl.Entries[idx:]))
        copy(result, tl.Entries[idx:])
        return result
    }
    return nil
}