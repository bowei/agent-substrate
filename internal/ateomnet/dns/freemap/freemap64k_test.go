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
	"math/rand/v2"
	"testing"
)

func TestGetIndex(t *testing.T) {
	tests := []struct {
		index uint16
		want  bitsetIndex
	}{
		{index: 0, want: bitsetIndex{l0: 0, l0Bit: 0, l1: 0, l1Bit: 0}},
		{index: 63, want: bitsetIndex{l0: 0, l0Bit: 63, l1: 0, l1Bit: 0}},
		{index: 64, want: bitsetIndex{l0: 1, l0Bit: 0, l1: 0, l1Bit: 1}},
		{index: 127, want: bitsetIndex{l0: 1, l0Bit: 63, l1: 0, l1Bit: 1}},
		{index: 4095, want: bitsetIndex{l0: 63, l0Bit: 63, l1: 0, l1Bit: 63}},
		{index: 4096, want: bitsetIndex{l0: 64, l0Bit: 0, l1: 1, l1Bit: 0}},
		{index: 65535, want: bitsetIndex{l0: 1023, l0Bit: 63, l1: 15, l1Bit: 63}},
	}

	for _, tc := range tests {
		got := getIndex(tc.index)
		if got != tc.want {
			t.Errorf("getIndex(%d) = %+v, want %+v", tc.index, got, tc.want)
		}
	}
}

func TestMap64k_Basic(t *testing.T) {
	var m Map64k
	if got := m.Count(); got != 0 {
		t.Fatalf("expected count 0, got %d", got)
	}
	if got := m.FindFirstUnset(); got != 0 {
		t.Fatalf("expected first unset 0, got %d", got)
	}
	if m.Get(0) {
		t.Fatalf("expected Get(0) = false initially")
	}

	m.Set(0)
	if got := m.Count(); got != 1 {
		t.Fatalf("expected count 1, got %d", got)
	}
	if !m.Get(0) {
		t.Fatalf("expected Get(0) = true after Set(0)")
	}
	if got := m.FindFirstUnset(); got != 1 {
		t.Fatalf("expected first unset 1, got %d", got)
	}

	// Idempotent Set
	m.Set(0)
	if got := m.Count(); got != 1 {
		t.Fatalf("expected count 1 after redundant set, got %d", got)
	}

	m.Clear(0)
	if got := m.Count(); got != 0 {
		t.Fatalf("expected count 0, got %d", got)
	}
	if m.Get(0) {
		t.Fatalf("expected Get(0) = false after Clear(0)")
	}
	if got := m.FindFirstUnset(); got != 0 {
		t.Fatalf("expected first unset 0, got %d", got)
	}

	// Idempotent Clear
	m.Clear(0)
	if got := m.Count(); got != 0 {
		t.Fatalf("expected count 0 after redundant clear, got %d", got)
	}
}

func TestMap64k_Get(t *testing.T) {
	var m Map64k
	testIndices := []uint16{0, 1, 63, 64, 127, 1024, 4095, 4096, 65534, 65535}

	for _, idx := range testIndices {
		if m.Get(idx) {
			t.Errorf("Get(%d) = true on empty map, want false", idx)
		}
		m.Set(idx)
		if !m.Get(idx) {
			t.Errorf("Get(%d) = false after Set, want true", idx)
		}
		m.Clear(idx)
		if m.Get(idx) {
			t.Errorf("Get(%d) = true after Clear, want false", idx)
		}
	}
}

func TestMap64k_HierarchyPropagation(t *testing.T) {
	var m Map64k

	// 1. Fill word 0 (bits 0..63). L1 bit 0 should be set, L2 should still be 0.
	for i := range uint16(64) {
		m.Set(i)
	}
	if (m.l1[0] & 1) == 0 {
		t.Errorf("expected l1[0] bit 0 to be set")
	}
	if m.l2 != 0 {
		t.Errorf("expected l2 to be 0, got %x", m.l2)
	}

	// Clearing one bit in word 0 should clear L1 bit 0.
	m.Clear(10)
	if (m.l1[0] & 1) != 0 {
		t.Errorf("expected l1[0] bit 0 to be cleared after clearing bit 10")
	}

	// Re-setting bit 10 should restore L1 bit 0.
	m.Set(10)
	if (m.l1[0] & 1) == 0 {
		t.Errorf("expected l1[0] bit 0 to be set after re-setting bit 10")
	}

	// 2. Fill all 4096 bits in L1 word 0 (words 0..63). L2 bit 0 should be set.
	for i := uint16(64); i < 4096; i++ {
		m.Set(i)
	}
	if (m.l2 & 1) == 0 {
		t.Errorf("expected l2 bit 0 to be set after filling 4096 bits")
	}

	// Clearing one bit should clear L1 bit and propagate to L2 bit 0.
	m.Clear(2000)
	if (m.l2 & 1) != 0 {
		t.Errorf("expected l2 bit 0 to be cleared after clearing bit 2000")
	}
}

func TestMap64k_FindFirstUnsetFrom_Empty(t *testing.T) {
	var m Map64k
	testIndices := []uint16{0, 1, 63, 64, 100, 1023, 1024, 4095, 4096, 65535}
	for _, idx := range testIndices {
		if got := m.FindFirstUnsetFrom(idx); got != int(idx) {
			t.Errorf("FindFirstUnsetFrom(%d) on empty map = %d, want %d", idx, got, idx)
		}
	}
}

func TestMap64k_FindFirstUnsetFrom_Full(t *testing.T) {
	var m Map64k
	for i := range numBits {
		m.Set(uint16(i))
	}
	if got := m.Count(); got != numBits {
		t.Fatalf("expected count %d, got %d", numBits, got)
	}
	if got := m.FindFirstUnset(); got != -1 {
		t.Fatalf("FindFirstUnset on full map = %d, want -1", got)
	}
	for _, idx := range []uint16{0, 1, 100, 4096, 65535} {
		if got := m.FindFirstUnsetFrom(idx); got != -1 {
			t.Errorf("FindFirstUnsetFrom(%d) on full map = %d, want -1", idx, got)
		}
	}
}

func TestMap64k_FindFirstUnsetFrom_SingleHole(t *testing.T) {
	var m Map64k
	for i := range numBits {
		m.Set(uint16(i))
	}

	testHoles := []uint16{0, 1, 63, 64, 127, 128, 4095, 4096, 32768, 65534, 65535}
	for _, hole := range testHoles {
		m.Clear(hole)
		if got := m.FindFirstUnset(); got != int(hole) {
			t.Errorf("hole %d: FindFirstUnset = %d, want %d", hole, got, hole)
		}

		// Starting from any index should find the only hole
		for _, start := range []uint16{0, hole, uint16((int(hole) + 1) % numBits), 65535} {
			if got := m.FindFirstUnsetFrom(start); got != int(hole) {
				t.Errorf("hole %d, start %d: FindFirstUnsetFrom = %d, want %d", hole, start, got, hole)
			}
		}

		m.Set(hole)
	}
}

func TestMap64k_FindFirstUnsetFrom_Levels(t *testing.T) {
	t.Run("same L0 word", func(t *testing.T) {
		var m Map64k
		// Fill bits 0..9
		for i := range uint16(10) {
			m.Set(i)
		}
		// Search from 0 should find 10
		if got := m.FindFirstUnsetFrom(0); got != 10 {
			t.Errorf("got %d, want 10", got)
		}
		// Search from 5 should find 10
		if got := m.FindFirstUnsetFrom(5); got != 10 {
			t.Errorf("got %d, want 10", got)
		}
		// Search from 10 should find 10
		if got := m.FindFirstUnsetFrom(10); got != 10 {
			t.Errorf("got %d, want 10", got)
		}
	})

	t.Run("next L0 word in same L1 word", func(t *testing.T) {
		var m Map64k
		// Fill L0 word 0 (bits 0..63) and bits 0..4 of word 1 (bits 64..68)
		for i := range uint16(69) {
			m.Set(i)
		}
		if got := m.FindFirstUnsetFrom(0); got != 69 {
			t.Errorf("got %d, want 69", got)
		}
		if got := m.FindFirstUnsetFrom(63); got != 69 {
			t.Errorf("got %d, want 69", got)
		}
		if got := m.FindFirstUnsetFrom(64); got != 69 {
			t.Errorf("got %d, want 69", got)
		}
	})

	t.Run("next L1 word from L2", func(t *testing.T) {
		var m Map64k
		// Fill entire L1 word 0 (L0 words 0..63, which is bits 0..4095)
		for i := range uint16(4096) {
			m.Set(i)
		}
		// Set bits 4096..4099 in L1 word 1
		for i := uint16(4096); i < 4100; i++ {
			m.Set(i)
		}
		if got := m.FindFirstUnsetFrom(0); got != 4100 {
			t.Errorf("got %d, want 4100", got)
		}
		if got := m.FindFirstUnsetFrom(2000); got != 4100 {
			t.Errorf("got %d, want 4100", got)
		}
		if got := m.FindFirstUnsetFrom(4095); got != 4100 {
			t.Errorf("got %d, want 4100", got)
		}
		if got := m.FindFirstUnsetFrom(4096); got != 4100 {
			t.Errorf("got %d, want 4100", got)
		}
	})

	t.Run("wrap around", func(t *testing.T) {
		var m Map64k
		// Fill bits 100 to 65535
		for i := 100; i < numBits; i++ {
			m.Set(uint16(i))
		}
		// Also fill bits 0..9
		for i := range uint16(10) {
			m.Set(i)
		}
		// Search from 100 should wrap around and find 10
		if got := m.FindFirstUnsetFrom(100); got != 10 {
			t.Errorf("got %d, want 10", got)
		}
		if got := m.FindFirstUnsetFrom(65535); got != 10 {
			t.Errorf("got %d, want 10", got)
		}
	})

	t.Run("wrap around with last bit set", func(t *testing.T) {
		var m Map64k
		// Fill only the very last bit
		m.Set(65535)
		if got := m.FindFirstUnsetFrom(65535); got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})
}

func TestMap64k_FindFirstUnsetRange(t *testing.T) {
	var m Map64k

	// Empty ranges
	if got := m.findFirstUnsetRange(5, 5); got != -1 {
		t.Errorf("range [5, 5) = %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(10, 5); got != -1 {
		t.Errorf("range [10, 5) = %d, want -1", got)
	}

	// Range within single word
	for i := uint16(10); i < 20; i++ {
		m.Set(i)
	}
	if got := m.findFirstUnsetRange(10, 20); got != -1 {
		t.Errorf("range [10, 20) fully set, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(10, 25); got != 20 {
		t.Errorf("range [10, 25), got %d, want 20", got)
	}
	if got := m.findFirstUnsetRange(5, 20); got != 5 {
		t.Errorf("range [5, 20), got %d, want 5", got)
	}

	// Range spanning word boundary
	for i := uint16(60); i < 64; i++ {
		m.Set(i)
	}
	// Bits 60..63 are set, 64 is unset
	if got := m.findFirstUnsetRange(60, 64); got != -1 {
		t.Errorf("range [60, 64) fully set, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(60, 70); got != 64 {
		t.Errorf("range [60, 70), got %d, want 64", got)
	}

	// Range spanning L1 boundary (word 63 to word 64, bit 4095 to 4096)
	for i := uint16(4090); i < 4096; i++ {
		m.Set(i)
	}
	if got := m.findFirstUnsetRange(4090, 4096); got != -1 {
		t.Errorf("range [4090, 4096) fully set, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(4090, 4100); got != 4096 {
		t.Errorf("range [4090, 4100), got %d, want 4096", got)
	}

	// Full span
	if got := m.findFirstUnsetRange(0, numBits); got != 0 {
		t.Errorf("range [0, numBits), got %d, want 0", got)
	}

	// Full map
	for i := range numBits {
		m.Set(uint16(i))
	}
	if got := m.findFirstUnsetRange(0, numBits); got != -1 {
		t.Errorf("range [0, numBits) on full map, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(100, 200); got != -1 {
		t.Errorf("range [100, 200) on full map, got %d, want -1", got)
	}
}

// TestMap64k_Differential against a naive linear search on a boolean slice.
func TestMap64k_Differential(t *testing.T) {
	var m Map64k
	occupied := make([]bool, numBits)

	naiveFindFirstUnsetRange := func(start uint16, end int) int {
		for idx := int(start); idx < end; idx++ {
			if !occupied[idx] {
				return idx
			}
		}
		return -1
	}

	naiveFindFirstUnsetFrom := func(start uint16) int {
		if idx := naiveFindFirstUnsetRange(start, numBits); idx != -1 {
			return idx
		}
		return naiveFindFirstUnsetRange(0, int(start))
	}

	r := rand.New(rand.NewPCG(42, 1337))

	for step := range 5000 {
		op := r.IntN(10)
		switch {
		case op < 4: // Set a random bit
			idx := uint16(r.IntN(numBits))
			m.Set(idx)
			occupied[idx] = true
		case op < 6: // Clear a random bit
			idx := uint16(r.IntN(numBits))
			m.Clear(idx)
			occupied[idx] = false
		case op < 8: // Query range
			start := uint16(r.IntN(numBits))
			end := int(start) + r.IntN(numBits-int(start)+1)
			expected := naiveFindFirstUnsetRange(start, end)
			got := m.findFirstUnsetRange(start, end)
			if got != expected {
				t.Fatalf("step %d: findFirstUnsetRange(%d, %d) = %d, want %d", step, start, end, got, expected)
			}
		default: // Query from
			start := uint16(r.IntN(numBits))
			expected := naiveFindFirstUnsetFrom(start)
			got := m.FindFirstUnsetFrom(start)
			if got != expected {
				t.Fatalf("step %d: FindFirstUnsetFrom(%d) = %d, want %d", step, start, got, expected)
			}
		}
	}
}

func TestMap64k_Differential_Dense(t *testing.T) {
	var m Map64k
	occupied := make([]bool, numBits)

	naiveFindFirstUnsetRange := func(start uint16, end int) int {
		for idx := int(start); idx < end; idx++ {
			if !occupied[idx] {
				return idx
			}
		}
		return -1
	}

	naiveFindFirstUnsetFrom := func(start uint16) int {
		if idx := naiveFindFirstUnsetRange(start, numBits); idx != -1 {
			return idx
		}
		return naiveFindFirstUnsetRange(0, int(start))
	}

	r := rand.New(rand.NewPCG(99, 12345))

	// Pre-fill ~95% of bits
	for range 62000 {
		idx := uint16(r.IntN(numBits))
		m.Set(idx)
		occupied[idx] = true
	}

	for step := range 5000 {
		op := r.IntN(10)
		switch {
		case op < 3:
			idx := uint16(r.IntN(numBits))
			m.Set(idx)
			occupied[idx] = true
		case op < 5:
			idx := uint16(r.IntN(numBits))
			m.Clear(idx)
			occupied[idx] = false
		case op < 7:
			start := uint16(r.IntN(numBits))
			end := int(start) + r.IntN(numBits-int(start)+1)
			expected := naiveFindFirstUnsetRange(start, end)
			got := m.findFirstUnsetRange(start, end)
			if got != expected {
				t.Fatalf("dense step %d: findFirstUnsetRange(%d, %d) = %d, want %d", step, start, end, got, expected)
			}
		default:
			start := uint16(r.IntN(numBits))
			expected := naiveFindFirstUnsetFrom(start)
			got := m.FindFirstUnsetFrom(start)
			if got != expected {
				t.Fatalf("dense step %d: FindFirstUnsetFrom(%d) = %d, want %d", step, start, got, expected)
			}
		}
	}
}

func BenchmarkFindFirstUnsetFrom(b *testing.B) {
	var m Map64k
	r := rand.New(rand.NewPCG(1, 2))
	for range 30000 {
		m.Set(uint16(r.IntN(numBits)))
	}

	b.ResetTimer()
	for i := range b.N {
		m.FindFirstUnsetFrom(uint16(i % numBits))
	}
}

func BenchmarkFindFirstUnsetRange(b *testing.B) {
	var m Map64k
	r := rand.New(rand.NewPCG(1, 2))
	for range 30000 {
		m.Set(uint16(r.IntN(numBits)))
	}

	b.ResetTimer()
	for i := range b.N {
		start := uint16(i % (numBits - 100))
		m.findFirstUnsetRange(start, int(start)+100)
	}
}
