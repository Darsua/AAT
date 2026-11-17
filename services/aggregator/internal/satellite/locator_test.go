package satellite

import "testing"

func TestNearestCentroidLocator(t *testing.T) {
	centroids, err := ParseProvinceCentroids([]byte(`[
		{"name":"Kalimantan Tengah","latitude":-1.68,"longitude":113.38},
		{"name":"Riau","latitude":0.29,"longitude":101.71},
		{"name":"Sumatera Selatan","latitude":-3.32,"longitude":104.91}
	]`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	locate := NearestCentroidLocator(centroids)
	for _, tc := range []struct {
		lat, lon float64
		want     string
	}{
		{-2.0, 113.5, "Kalimantan Tengah"},
		{0.9, 101.4, "Riau"},
		{-3.0, 105.2, "Sumatera Selatan"},
	} {
		if got := locate(tc.lat, tc.lon); got != tc.want {
			t.Errorf("locate(%v, %v) = %q, want %q", tc.lat, tc.lon, got, tc.want)
		}
	}

	f := Fire{ID: "fire-1", Latitude: -2.0, Longitude: 113.5}
	if h := f.ToHazard(locate); h.AreaName != "Kalimantan Tengah" {
		t.Fatalf("area_name = %q", h.AreaName)
	}
}

func TestParseProvinceCentroidsRejectsBadTables(t *testing.T) {
	for _, in := range []string{`[]`, `[{"name":"","latitude":0,"longitude":0}]`, `[{"name":"X","latitude":91,"longitude":0}]`, `{`} {
		if _, err := ParseProvinceCentroids([]byte(in)); err == nil {
			t.Errorf("ParseProvinceCentroids(%s) accepted a bad table", in)
		}
	}
}
