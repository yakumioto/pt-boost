package config

import (
	"strings"
	"testing"
)

func TestParse_ColdRateThresholdDefaultAndOverrides(t *testing.T) {
	for _, tt := range []struct {
		name, value string
		want        int64
		invalid     bool
	}{
		{name: "omitted", want: 100 * 1024},
		{name: "explicitDefault", value: "100KiB/s", want: 100 * 1024},
		{name: "override", value: "200KiB/s", want: 200 * 1024},
		{name: "decimalCompatibility", value: "50KB/s", want: 50000},
		{name: "zero", value: "0KiB/s", invalid: true},
		{name: "negative", value: "-1KiB/s", invalid: true},
		{name: "malformed", value: "invalid", invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			replacement := ""
			if tt.value != "" {
				replacement = "    cold_rate_threshold: " + tt.value + "\n"
			}
			data := strings.Replace(minimalValidYAML, "    cold_rate_threshold: 50KB/s\n", replacement, 1)
			cfg, err := Parse([]byte(data))
			if tt.invalid {
				if err == nil || !strings.Contains(err.Error(), "cold_rate_threshold") {
					t.Fatalf("expected named error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.Sites[0].ColdRateThreshold.BytesPerSecond; got != tt.want {
				t.Fatalf("rate=%d want %d", got, tt.want)
			}
		})
	}
}
