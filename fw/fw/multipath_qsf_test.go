package fw

import "testing"

func TestNormalizeDtQueueSignal(t *testing.T) {
	tests := []struct {
		name     string
		raw      float64
		capacity float64
		want     float64
	}{
		{name: "empty", raw: 0, capacity: dtInterestQueueCapacityPackets, want: 0},
		{name: "iq half", raw: 10, capacity: dtInterestQueueCapacityPackets, want: 0.5},
		{name: "iq high", raw: 16, capacity: dtInterestQueueCapacityPackets, want: 0.8},
		{name: "qdisc half", raw: 500, capacity: dtQdiscQueueCapacityPackets, want: 0.5},
		{name: "qdisc high", raw: 800, capacity: dtQdiscQueueCapacityPackets, want: 0.8},
		{name: "clamped", raw: 1200, capacity: dtQdiscQueueCapacityPackets, want: 1.0},
		{name: "invalid capacity", raw: 5, capacity: 0, want: 0},
		{name: "negative raw", raw: -1, capacity: dtInterestQueueCapacityPackets, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeDtQueueSignal(tc.raw, tc.capacity); got != tc.want {
				t.Fatalf("normalizeDtQueueSignal(%v, %v) = %v, want %v", tc.raw, tc.capacity, got, tc.want)
			}
		})
	}
}

func TestConditionDtDataQsfClampsNormalizedRange(t *testing.T) {
	tests := []struct {
		name string
		raw  float64
		want float64
	}{
		{name: "below zero", raw: -0.5, want: 0},
		{name: "in range", raw: 0.6, want: 0.6},
		{name: "above one", raw: 1.4, want: 1.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := conditionDtDataQsf("qdisc", tc.raw); got != tc.want {
				t.Fatalf("conditionDtDataQsf(%q, %v) = %v, want %v", "qdisc", tc.raw, got, tc.want)
			}
		})
	}
}
