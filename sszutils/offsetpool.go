// Copyright (c) 2025 pk910
// SPDX-License-Identifier: Apache-2.0
// This file is part of the dynamic-ssz library.

package sszutils

import "sync"

const (
	minOffsetSliceCap = 32
	maxOffsetSliceCap = 4096
)

var offsetSliceCapClasses = [...]int{32, 64, 128, 256, 512, 1024, 2048, 4096}

type offsetSliceBucket struct {
	capacity int
	pool     sync.Pool
}

// offsetSlicePool manages reusable int slices to reduce allocations
type offsetSlicePool struct {
	buckets [len(offsetSliceCapClasses)]offsetSliceBucket
}

// defaultOffsetSlicePool is the default int slice pool instance
var defaultOffsetSlicePool = newOffsetSlicePool()

func newOffsetSlicePool() *offsetSlicePool {
	pool := &offsetSlicePool{}
	for i, capacity := range offsetSliceCapClasses {
		capacity := capacity
		pool.buckets[i] = offsetSliceBucket{
			capacity: capacity,
			pool: sync.Pool{
				New: func() interface{} {
					return make([]uint32, 0, capacity)
				},
			},
		}
	}
	return pool
}

// Get returns an int slice from the pool, consumer can grow it as needed
func (p *offsetSlicePool) Get(size int) []uint32 {
	requestSize := size
	if requestSize < 0 {
		requestSize = 0
	}

	if bucket := p.bucketForSize(requestSize); bucket != nil {
		item, _ := bucket.pool.Get().([]uint32)
		slice := item[:requestSize]
		clear(slice)
		return slice
	}

	return make([]uint32, requestSize)
}

// Put returns an int slice to the pool
func (p *offsetSlicePool) Put(slice []uint32) {
	if bucket := p.bucketForCap(cap(slice)); bucket != nil {
		bucket.pool.Put(slice[:0])
	}
}

func (p *offsetSlicePool) bucketForSize(size int) *offsetSliceBucket {
	if size <= 0 {
		size = minOffsetSliceCap
	}

	for i := range p.buckets {
		bucket := &p.buckets[i]
		if size <= bucket.capacity {
			return bucket
		}
	}

	return nil
}

func (p *offsetSlicePool) bucketForCap(capacity int) *offsetSliceBucket {
	if capacity < minOffsetSliceCap || capacity > maxOffsetSliceCap {
		return nil
	}

	for i := range p.buckets {
		bucket := &p.buckets[i]
		if capacity == bucket.capacity {
			return bucket
		}
	}

	return nil
}

// GetOffsetSlice returns a uint32 slice of the given size from a shared pool,
// suitable for use as an SSZ offset buffer. The caller must return it via
// PutOffsetSlice when done.
func GetOffsetSlice(size int) []uint32 {
	return defaultOffsetSlicePool.Get(size)
}

// PutOffsetSlice returns a uint32 offset slice to the shared pool for reuse.
func PutOffsetSlice(slice []uint32) {
	defaultOffsetSlicePool.Put(slice)
}
