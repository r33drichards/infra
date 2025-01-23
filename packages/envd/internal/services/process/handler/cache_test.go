package handler

import (
	"testing"
	"time"
)

// test forker
type testForker[T any] struct {
	source chan T
}

func (f *testForker[T]) Fork() (chan T, func()) {
	return f.source, func() {}
}

func (f *testForker[T]) Source() chan T {
	return f.source
}

func Test_CachedMultiplex_PopulateEntriesAfter(t *testing.T) {
	tests := []struct {
		name      string
		timestamp time.Time
		inital    *TimestampedList[string]
		want      []TimestampedData[string]
	}{
		{
			name:      "entries after timestamp get returned",
			timestamp: time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC),
			inital: &TimestampedList[string]{
				Entries: []TimestampedData[string]{
					{Timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC), Data: "Data1"},
					{Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), Data: "Data2"},
					{Timestamp: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Data: "Data3"},
				},
			},

			want: []TimestampedData[string]{
				{Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), Data: "Data2"},
				{Timestamp: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Data: "Data3"},
			},
		},
		{
			name:      "no entries after timestamp",
			timestamp: time.Date(2025, 1, 1, 15, 0, 0, 0, time.UTC),
			inital: &TimestampedList[string]{
				Entries: []TimestampedData[string]{
					{Timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC), Data: "Data1"},
					{Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), Data: "Data2"},
					{Timestamp: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Data: "Data3"},
				},
			},

			want: []TimestampedData[string]{},
		},
		{
			name:      "all entries after timestamp",
			timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
			inital: &TimestampedList[string]{
				Entries: []TimestampedData[string]{
					{Timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC), Data: "Data1"},
					{Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), Data: "Data2"},
					{Timestamp: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Data: "Data3"},
				},
			},

			want: []TimestampedData[string]{
				{Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), Data: "Data2"},
				{Timestamp: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Data: "Data3"},
			},
		},
		{
			name:      "no entries",
			timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
			inital:    &TimestampedList[string]{},
			want:      []TimestampedData[string]{},
		},
		{
			name:      "one entry matches timestamp",
			timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
			inital: &TimestampedList[string]{
				Entries: []TimestampedData[string]{
					{Timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC), Data: "Data1"},
				},
			},
			want: []TimestampedData[string]{},
		},

		{
			name:      "one entry after timestamp",
			timestamp: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
			inital: &TimestampedList[string]{
				Entries: []TimestampedData[string]{
					{Timestamp: time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC), Data: "Data1"},
				},
			},

			want: []TimestampedData[string]{
				{Timestamp: time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC), Data: "Data1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testChan := make(chan string, len(tt.want))
			cm := CachedMultiplex[string]{
				forker:          &testForker[string]{source: testChan},
				timestampedList: tt.inital,
			}

			ch := cm.PopulateEntriesAfter(tt.timestamp).Source()

			close(testChan)

			for _, want := range tt.want {
				got, ok := <-ch
				if !ok {
					t.Errorf("got closed channel, want %s", want.Data)
				}
				if got != want.Data {
					t.Errorf("got %s, want %s", got, want.Data)
				}
			}

		})
	}
}

// testing this behavior by creating a CachedMultiplex and sending values to the source channel
// then asserting that the values are stored in the TimestampedList
func Test_CachedMultiplex_start(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{
			name:   "store values in timestamped list",
			values: []string{"Data1", "Data2", "Data3"},
		},
		{
			name:   "no values",
			values: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testChan := make(chan string)
			cm := CachedMultiplex[string]{
				forker:          &testForker[string]{source: testChan},
				timestampedList: &TimestampedList[string]{},
			}

			go cm.start()

			// Send values to testChan
			for _, v := range tt.values {
				testChan <- v
			}
			close(testChan)
			// lock the mutex to read the stored values
			cm.mu.RLock()
			// unlock the mutex
			defer cm.mu.RUnlock()
			// Verify stored values
			if len(cm.timestampedList.Entries) != len(tt.values) {
				t.Errorf("got %d entries, want %d", len(cm.timestampedList.Entries), len(tt.values))
			}

			for i, v := range tt.values {
				if i >= len(cm.timestampedList.Entries) {
					break
				}
				got := cm.timestampedList.Entries[i]
				if got.Data != v {
					t.Errorf("got %s, want %s", got.Data, v)
				}
				if got.Timestamp.IsZero() {
					t.Error("got zero timestamp")
				}
			}
		})
	}
}

