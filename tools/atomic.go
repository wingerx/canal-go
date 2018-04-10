package tools

import (
	"sync"
	"sync/atomic"
)

// AtomicBool gives an atomic boolean variable.
type AtomicBool struct {
	int32
}

// NewAtomicBool initializes a new AtomicBool with a given value.
func NewAtomicBool(n bool) AtomicBool {
	if n {
		return AtomicBool{1}
	}
	return AtomicBool{0}
}

// Set atomically sets n as new value.
func (i *AtomicBool) Set(n bool) {
	if n {
		atomic.StoreInt32(&i.int32, 1)
	} else {
		atomic.StoreInt32(&i.int32, 0)
	}
}

// Get atomically returns the current value.
func (i *AtomicBool) Get() bool {
	return atomic.LoadInt32(&i.int32) != 0
}

// AtomicString gives you atomic-style APIs for string, but
// it's only a convenience wrapper that uses a mutex. So, it's
// not as efficient as the rest of the atomic types.
type AtomicString struct {
	mu  sync.Mutex
	str string
}

// Set atomically sets str as new value.
func (s *AtomicString) Set(str string) {
	s.mu.Lock()
	s.str = str
	s.mu.Unlock()
}

// Get atomically returns the current value.
func (s *AtomicString) Get() string {
	s.mu.Lock()
	str := s.str
	s.mu.Unlock()
	return str
}

// CompareAndSwap atomatically swaps the old with the new value.
func (s *AtomicString) CompareAndSwap(oldval, newval string) (swqpped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.str == oldval {
		s.str = newval
		return true
	}
	return false
}
