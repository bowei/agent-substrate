// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package freemap

import (
	"math/bits"
)

const (
	numBits  = 65536
	wordSize = 64
	l0words  = numBits / wordSize // 1024 words
	l1words  = l0words / wordSize // 16 words
)

// Map64k is a free map that supports 64k entries. This data structure
// uses ~8kb of memory.
//
// This data structure is not thread-safe.
//
// This is represented as a hierarchical bit-set.
type Map64k struct {
	// Layer 0: Leaf-level storage (1 = occupied, 0 = empty)
	l0 [l0words]uint64
	// Layer 1: Intermediate summary (1 = corresponding L0 word is fully occupied)
	l1 [l1words]uint64
	// Layer 2: Root summary (1 = corresponding L1 word is fully occupied)
	l2 uint64
	// count is the number of bits currently set in the map.
	count uint
}

// Count returns the number of bits set in the map.
func (hb *Map64k) Count() uint { return hb.count }

// bitsetIndex into the hierarchical bitset.
type bitsetIndex struct {
	l0, l0Bit int
	l1, l1Bit int
}

// getIndex computes the index across all of the hierarchical bitsets.
func getIndex(index uint16) bitsetIndex {
	return bitsetIndex{
		l0:    int(index / wordSize),
		l0Bit: int(index % wordSize),
		l1:    int(index / wordSize / wordSize),
		l1Bit: int((index / wordSize) % wordSize),
	}
}

// Get return true if the index is Set.
func (hb *Map64k) Get(index uint16) bool {
	sidx := getIndex(index)
	return hb.l0[sidx.l0]&(1<<sidx.l0Bit) != 0
}

// Set marks a specific slot index.
func (hb *Map64k) Set(index uint16) {
	sidx := getIndex(index)

	// Bail out early if the slot was already set.
	if hb.l0[sidx.l0]&(1<<sidx.l0Bit) != 0 {
		return
	}

	hb.count++
	hb.l0[sidx.l0] |= (1 << sidx.l0Bit)

	// If the L0 is full, mark l1.
	if hb.l0[sidx.l0] == ^uint64(0) {
		hb.l1[sidx.l1] |= (1 << sidx.l1Bit)
		// If the L1 is full, mark l2.
		if hb.l1[sidx.l1] == ^uint64(0) {
			hb.l2 |= (1 << sidx.l1)
		}
	}
}

// Clear marks a specific slot index as empty (0).
func (hb *Map64k) Clear(index uint16) {
	sidx := getIndex(index)

	if hb.l0[sidx.l0]&(1<<sidx.l0Bit) == 0 {
		// Bit was already clear, return.
		return
	}

	hb.count--
	wasFull := hb.l0[sidx.l0] == ^uint64(0)
	hb.l0[sidx.l0] &= ^(1 << sidx.l0Bit)

	// If this word was full but is no longer full, propagate the update.
	if wasFull {
		l1WasFull := hb.l1[sidx.l1] == ^uint64(0)
		hb.l1[sidx.l1] &= ^(1 << sidx.l1Bit)
		if l1WasFull {
			hb.l2 &= ^(1 << sidx.l1)
		}
	}
}

// FindFirstUnset returns the index of the first empty (0) bit.
//
// Returns -1 if the entire 64k bitset is completely occupied.
func (hb *Map64k) FindFirstUnset() int { return hb.findFirstUnsetRange(0, numBits) }

// FindFirstUnsetFrom returns the index of the first empty (0) bit, starting
// search from `index`. Search will wrap around the end of the ID space.
//
// Returns -1 if the entire 64k bitset is completely occupied.
func (hb *Map64k) FindFirstUnsetFrom(index uint16) int {
	// Search from index to the end.
	idx := hb.findFirstUnsetRange(index, numBits)
	if idx != -1 {
		return idx
	}
	// Wrap around to search from 0 to index.
	idx = hb.findFirstUnsetRange(0, int(index))
	if idx != -1 {
		return idx
	}
	return -1
}

// findFirstUnsetRange searches for the first empty (0) bit in the range [start, end).
//
// Returns -1 if the entire range is completely occupied.
func (hb *Map64k) findFirstUnsetRange(start uint16, end int) int {
	if int(start) >= end {
		return -1
	}

	// 1. Check if the entire map is occupied.
	if (hb.l2 & 0xFFFF) == 0xFFFF {
		return -1
	}

	startIdx := getIndex(start)

	// 2. Scan remaining bits in the current Layer 0 word.
	w0 := hb.l0[startIdx.l0] | ((uint64(1) << startIdx.l0Bit) - 1)
	if inv0 := ^w0; inv0 != 0 {
		idx := (startIdx.l0 * wordSize) + bits.TrailingZeros64(inv0)
		if idx < end {
			return idx
		}
		return -1
	}

	// 3. Scan remaining Layer 0 words in the current Layer 1 word.
	nextL0Word := startIdx.l0 + 1
	if nextL0Word < l0words && nextL0Word*wordSize < end {
		l1Word := nextL0Word / wordSize
		l1Bit := nextL0Word % wordSize

		w1 := hb.l1[l1Word] | ((uint64(1) << l1Bit) - 1)
		if inv1 := ^w1; inv1 != 0 {
			l0WordInL1 := bits.TrailingZeros64(inv1)
			targetL0 := (l1Word * wordSize) + l0WordInL1
			idx := (targetL0 * wordSize) + bits.TrailingZeros64(^hb.l0[targetL0])
			if idx < end {
				return idx
			}
			return -1
		}

		// 4. Scan remaining Layer 1 words in Layer 2 (Root).
		nextL1Word := l1Word + 1
		if nextL1Word < l1words && nextL1Word*wordSize*wordSize < end {
			w2 := hb.l2 | ((uint64(1) << nextL1Word) - 1)
			if inv2 := (^w2) & 0xFFFF; inv2 != 0 {
				targetL1 := bits.TrailingZeros64(inv2)
				l0WordInL1 := bits.TrailingZeros64(^hb.l1[targetL1])
				targetL0 := (targetL1 * wordSize) + l0WordInL1
				idx := (targetL0 * wordSize) + bits.TrailingZeros64(^hb.l0[targetL0])
				if idx < end {
					return idx
				}
				return -1
			}
		}
	}

	return -1
}
