package rate

import "testing"

func TestRateCacheKeyIncludesStayDates(t *testing.T) {
	keyA := rateCacheKey("1", "2015-04-09", "2015-04-10")
	keyB := rateCacheKey("1", "2015-04-17", "2015-04-18")

	if keyA == keyB {
		t.Fatalf("expected cache keys to differ by stay date, got %q", keyA)
	}
}
