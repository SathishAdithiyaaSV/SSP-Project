package search

import (
	"testing"

	rate "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
)

func TestBuildSearchResultDeduplicatesAndFiltersHotelIDs(t *testing.T) {
	nearbyHotelIDs := []string{"1", "2", "3"}
	ratePlans := []*rate.RatePlan{
		{HotelId: "2"},
		{HotelId: "2"},
		{HotelId: "9"},
		{HotelId: "1"},
		{HotelId: "3"},
		{HotelId: "1"},
	}

	result := buildSearchResult(nearbyHotelIDs, ratePlans)

	want := []string{"2", "1", "3"}
	if len(result.HotelIds) != len(want) {
		t.Fatalf("unexpected hotel count: got %d want %d (%v)", len(result.HotelIds), len(want), result.HotelIds)
	}
	for i, hotelID := range want {
		if result.HotelIds[i] != hotelID {
			t.Fatalf("unexpected hotel at %d: got %q want %q (%v)", i, result.HotelIds[i], hotelID, result.HotelIds)
		}
	}
}
