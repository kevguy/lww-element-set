package lww

import (
	"fmt"
	"sync"
	"time"
	"unsafe"
)

const MaxStringInBytes = 1 << 29 // 512MB

var (
	ElementSizeError = fmt.Errorf("element string exceeds the maximum length in bytes: %d", MaxStringInBytes)
)

// LwwSet represents the Last-Writer-Win element set.
//
// This set stores only one instance of each element, and associates
// each element with a timestamp, i.e. (lww_set element, timestamp).
type LwwSet struct {
	AddSet		map[string]time.Time
	RemoveSet	map[string]time.Time
	addLock 	sync.Mutex
	removeLock	sync.Mutex
}

// New constructs a new LwwSet.
func New() LwwSet {
	return LwwSet{
		AddSet:     map[string]time.Time{},
		RemoveSet:  map[string]time.Time{},
		addLock:    sync.Mutex{},
		removeLock: sync.Mutex{},
	}
}

func validateElement(el string) (string, error) {
	if len(el) + int(unsafe.Sizeof(el)) > MaxStringInBytes {
		return "", ElementSizeError
	}
	return el, nil
}

//
// Add adds an element to the LwwSet, or update the existing element timestamp
//
// If the operation has the most recent timestamp, the operation will
// eventually succeed. Otherwise, when other processes or threads are invoking
// Add() or Remove() operations concurrently with this method, the timestamp
// of an element may be overwritten by another newer timestamp. Therefore,
// therefore, there is no guarantee that this method succeeds. The return
// value is always none and does not indicate the success of this operation.
//
// Arguments:
// el: an object that has a unique identifier
// timestamp: the timestamp
//
// Returns:
// true: the operation is acknowledged and processed by the data structure
//       according to CRDT semantic, but not guarantee it has taken effect.
// false: there was an internal error during operation.
//        A retry may solve the problem.
//
func (lset *LwwSet) Add(el string, timestamp time.Time) error {
	lset.addLock.Lock()
	defer lset.addLock.Unlock()

	el, err := validateElement(el)
	if err != nil {
		return fmt.Errorf("failed to add element: %w", err)
	}

	val, found := lset.AddSet[el]
	if found {
		var currentTimestamp = val
		if currentTimestamp.Before(timestamp) {
			lset.AddSet[el] = timestamp
		}
	} else {
		lset.AddSet[el] = timestamp
	}
	return nil
}

func (lset *LwwSet) Remove(el string, timestamp time.Time) error {
	lset.removeLock.Lock()
	defer lset.removeLock.Unlock()

	el, err := validateElement(el)
	if err != nil {
		return fmt.Errorf("failed to remove element: %w", err)
	}

	val, found := lset.RemoveSet[el]
	if found {
		var currentTimestamp = val
		if currentTimestamp.Before(timestamp) {
			lset.RemoveSet[el] = timestamp
		}
	} else {
		lset.RemoveSet[el] = timestamp
	}
	return nil
}

// Exist checks if the element exists in the LwwSet.
//
// For an LwwSet:
// 1. An element is in the set if its most recent operation was an add,
//    or when add or remove operation have the same timestamp (i.e. we bias add)
// 2. An element is not in the set if its most recent operation was an remove,
//    or there exists no such element in either add or remove set.
//
// The method is read-only. When other processes/threads are calling Add() or Remove()
// concurrently, it is possible that this method does not return the most recent results.
// However, the result will be eventually up-to-date when all other operations
// actually complete.
func (lset LwwSet) Exist(el string) bool {
	if addSetTimestamp, found := lset.AddSet[el]; found {
		if removeSetTimestamp, found := lset.RemoveSet[el]; found {
			if addSetTimestamp.After(removeSetTimestamp) || addSetTimestamp.Equal(removeSetTimestamp) {
				return true
			} else {
				return false
			}
		} else {
			return true
		}
	} else {
		// el not found in AddSet
		return false
	}
}

// Get returns an array of all existing elements in the LwwSet
//
// Similar to Exist(), when this method is invoked concurrently
// with Add() or Remove() operations, it is possible that it does
// not return the most recent result. However, the result will be
// eventually up-to-date when other operations actually complete.
func (lset LwwSet) Get() []string {
	var results []string
	for el, _ := range lset.AddSet {
		if lset.Exist(el) {
			results = append(results, el)
		}
	}
	return results
}