package ranges

import (
	"testing"
)

func TestRangeBasics(t *testing.T) {
	r1 := Range{Pos: 0, Size: 10}
	r2 := Range{Pos: 5, Size: 10}
	r3 := Range{Pos: 20, Size: 10}

	// Test End()
	if r1.End() != 10 {
		t.Errorf("Expected end 10, got %d", r1.End())
	}

	// Test Overlaps()
	if !r1.Overlaps(r2) {
		t.Error("r1 should overlap r2")
	}
	if r1.Overlaps(r3) {
		t.Error("r1 should not overlap r3")
	}

	// Test Contains()
	r4 := Range{Pos: 2, Size: 5}
	if !r1.Contains(r4) {
		t.Error("r1 should contain r4")
	}
	if r1.Contains(r2) {
		t.Error("r1 should not contain r2 (partial overlap)")
	}
}

func TestInsertSingle(t *testing.T) {
	rs := New()

	rs.Insert(Range{Pos: 10, Size: 5})

	if rs.Len() != 1 {
		t.Fatalf("Expected 1 range, got %d", rs.Len())
	}

	ranges := rs.GetRanges()
	if ranges[0].Pos != 10 || ranges[0].Size != 5 {
		t.Errorf("Expected {10, 5}, got {%d, %d}", ranges[0].Pos, ranges[0].Size)
	}
}

func TestInsertCoalesce(t *testing.T) {
	rs := New()

	// Insert adjacent ranges - should coalesce
	rs.Insert(Range{Pos: 0, Size: 10})
	rs.Insert(Range{Pos: 10, Size: 10})

	if rs.Len() != 1 {
		t.Fatalf("Expected 1 coalesced range, got %d", rs.Len())
	}

	ranges := rs.GetRanges()
	if ranges[0].Pos != 0 || ranges[0].Size != 20 {
		t.Errorf("Expected {0, 20}, got {%d, %d}", ranges[0].Pos, ranges[0].Size)
	}
}

func TestInsertOverlapping(t *testing.T) {
	rs := New()

	// Insert overlapping ranges
	rs.Insert(Range{Pos: 0, Size: 15})
	rs.Insert(Range{Pos: 10, Size: 15})

	if rs.Len() != 1 {
		t.Fatalf("Expected 1 merged range, got %d", rs.Len())
	}

	ranges := rs.GetRanges()
	if ranges[0].Pos != 0 || ranges[0].Size != 25 {
		t.Errorf("Expected {0, 25}, got {%d, %d}", ranges[0].Pos, ranges[0].Size)
	}
}

func TestInsertMultipleGaps(t *testing.T) {
	rs := New()

	// Insert non-overlapping ranges
	rs.Insert(Range{Pos: 0, Size: 10})
	rs.Insert(Range{Pos: 20, Size: 10})
	rs.Insert(Range{Pos: 40, Size: 10})

	if rs.Len() != 3 {
		t.Fatalf("Expected 3 ranges, got %d", rs.Len())
	}

	// Fill the gaps
	rs.Insert(Range{Pos: 10, Size: 10})
	rs.Insert(Range{Pos: 30, Size: 10})

	if rs.Len() != 1 {
		t.Fatalf("Expected 1 coalesced range, got %d", rs.Len())
	}

	ranges := rs.GetRanges()
	if ranges[0].Pos != 0 || ranges[0].Size != 50 {
		t.Errorf("Expected {0, 50}, got {%d, %d}", ranges[0].Pos, ranges[0].Size)
	}
}

func TestPresent(t *testing.T) {
	rs := New()

	rs.Insert(Range{Pos: 10, Size: 20})
	rs.Insert(Range{Pos: 40, Size: 20})

	tests := []struct {
		r       Range
		present bool
	}{
		{Range{Pos: 10, Size: 20}, true},   // Exact match
		{Range{Pos: 15, Size: 10}, true},   // Subset
		{Range{Pos: 10, Size: 25}, false},  // Extends into gap
		{Range{Pos: 5, Size: 10}, false},   // Starts before
		{Range{Pos: 40, Size: 20}, true},   // Second range exact
		{Range{Pos: 25, Size: 10}, false},  // In gap
		{Range{Pos: 10, Size: 50}, false},  // Spans both with gap
	}

	for _, test := range tests {
		result := rs.Present(test.r)
		if result != test.present {
			t.Errorf("Present(%v) = %v, expected %v", test.r, result, test.present)
		}
	}
}

func TestFindMissing(t *testing.T) {
	rs := New()

	rs.Insert(Range{Pos: 10, Size: 10})
	rs.Insert(Range{Pos: 30, Size: 10})

	// Request range [0, 50) - should find missing [0, 10), [20, 30), [40, 50)
	missing := rs.FindMissing(Range{Pos: 0, Size: 50})

	expected := []Range{
		{Pos: 0, Size: 10},
		{Pos: 20, Size: 10},
		{Pos: 40, Size: 10},
	}

	if len(missing) != len(expected) {
		t.Fatalf("Expected %d missing ranges, got %d", len(expected), len(missing))
	}

	for i, exp := range expected {
		if missing[i] != exp {
			t.Errorf("Missing[%d] = %v, expected %v", i, missing[i], exp)
		}
	}
}

func TestFindMissingFullyCovered(t *testing.T) {
	rs := New()

	rs.Insert(Range{Pos: 0, Size: 100})

	// Request range fully covered
	missing := rs.FindMissing(Range{Pos: 10, Size: 20})

	if len(missing) != 0 {
		t.Errorf("Expected no missing ranges, got %d", len(missing))
	}
}

func TestFindMissingEmpty(t *testing.T) {
	rs := New()

	// No ranges inserted, everything is missing
	missing := rs.FindMissing(Range{Pos: 0, Size: 100})

	if len(missing) != 1 {
		t.Fatalf("Expected 1 missing range, got %d", len(missing))
	}

	if missing[0].Pos != 0 || missing[0].Size != 100 {
		t.Errorf("Expected {0, 100}, got {%d, %d}", missing[0].Pos, missing[0].Size)
	}
}

func TestSize(t *testing.T) {
	rs := New()

	rs.Insert(Range{Pos: 0, Size: 10})
	rs.Insert(Range{Pos: 20, Size: 15})
	rs.Insert(Range{Pos: 50, Size: 25})

	total := rs.Size()
	expected := int64(10 + 15 + 25)

	if total != expected {
		t.Errorf("Expected total size %d, got %d", expected, total)
	}
}

func TestVideoStreamingScenario(t *testing.T) {
	rs := New()

	// Simulate video player downloading chunks
	chunkSize := int64(8 * 1024 * 1024) // 8MB chunks

	// Download first 3 chunks (initial buffer)
	for i := int64(0); i < 3; i++ {
		rs.Insert(Range{Pos: i * chunkSize, Size: chunkSize})
	}

	// Check first chunk is present
	if !rs.Present(Range{Pos: 0, Size: chunkSize}) {
		t.Error("First chunk should be present")
	}

	// Check middle of file is not present
	midPoint := int64(50) * chunkSize
	if rs.Present(Range{Pos: midPoint, Size: chunkSize}) {
		t.Error("Middle chunk should not be present")
	}

	// User seeks to middle
	rs.Insert(Range{Pos: midPoint, Size: chunkSize})

	if !rs.Present(Range{Pos: midPoint, Size: chunkSize}) {
		t.Error("Middle chunk should now be present")
	}

	// Verify we have 4 separate ranges (not coalesced)
	if rs.Len() != 2 {
		t.Errorf("Expected 2 range groups, got %d", rs.Len())
	}
}

func TestConcurrentAccess(t *testing.T) {
	rs := New()

	// Spawn multiple goroutines inserting ranges
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(offset int64) {
			rs.Insert(Range{Pos: offset * 100, Size: 50})
			done <- true
		}(int64(i))
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify ranges were inserted
	if rs.Len() == 0 {
		t.Error("No ranges were inserted")
	}

	// Verify thread safety - no crashes
	t.Log("Concurrent access test passed")
}

func BenchmarkInsert(b *testing.B) {
	rs := New()
	chunkSize := int64(8 * 1024 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i) * chunkSize
		rs.Insert(Range{Pos: offset, Size: chunkSize})
	}
}

func BenchmarkPresent(b *testing.B) {
	rs := New()
	chunkSize := int64(8 * 1024 * 1024)

	// Pre-populate with 100 chunks
	for i := int64(0); i < 100; i++ {
		rs.Insert(Range{Pos: i * chunkSize, Size: chunkSize})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i%100) * chunkSize
		rs.Present(Range{Pos: offset, Size: chunkSize})
	}
}

func BenchmarkFindMissing(b *testing.B) {
	rs := New()
	chunkSize := int64(8 * 1024 * 1024)

	// Create sparse pattern: every other chunk
	for i := int64(0); i < 50; i++ {
		if i%2 == 0 {
			rs.Insert(Range{Pos: i * chunkSize, Size: chunkSize})
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs.FindMissing(Range{Pos: 0, Size: 50 * chunkSize})
	}
}
