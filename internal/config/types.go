package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration wraps time.Duration so call sites can use the parsed value
// directly (field.Duration) after it has been parsed from a YAML scalar
// such as "10m" or "3s" using the same syntax as time.ParseDuration.
type Duration struct {
	time.Duration
}

func parseDuration(s string) (Duration, error) {
	parsed, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return Duration{}, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return Duration{parsed}, nil
}

// ByteSize wraps a byte count, parsed from a YAML scalar such as "256MiB",
// "2GiB", "100KB", or a bare integer number of bytes.
type ByteSize struct {
	Bytes int64
}

func parseByteSizeField(s string) (ByteSize, error) {
	parsed, err := parseByteSize(s)
	if err != nil {
		return ByteSize{}, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	return ByteSize{parsed}, nil
}

// Rate wraps a bytes-per-second value, parsed from a YAML scalar such as
// "50KB/s".
type Rate struct {
	BytesPerSecond int64
}

func parseRate(s string) (Rate, error) {
	trimmed := strings.TrimSuffix(strings.TrimSpace(s), "/s")
	parsed, err := parseByteSize(trimmed)
	if err != nil {
		return Rate{}, fmt.Errorf("invalid rate %q: %w", s, err)
	}
	return Rate{parsed}, nil
}

// byteUnit associates a case-sensitive unit suffix with its multiplier in
// bytes. Longer suffixes must be listed before their prefixes (e.g. "KiB"
// before "KB" before "B") so the matching loop picks the most specific one
// first.
var byteUnits = []struct {
	suffix     string
	multiplier int64
}{
	{"GiB", 1024 * 1024 * 1024},
	{"MiB", 1024 * 1024},
	{"KiB", 1024},
	{"GB", 1000 * 1000 * 1000},
	{"MB", 1000 * 1000},
	{"KB", 1000},
	{"B", 1},
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}

	for _, unit := range byteUnits {
		if strings.HasSuffix(s, unit.suffix) {
			numPart := strings.TrimSpace(strings.TrimSuffix(s, unit.suffix))
			if numPart == "" {
				return 0, fmt.Errorf("missing number before unit %q", unit.suffix)
			}
			n, err := strconv.ParseFloat(numPart, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid number %q: %w", numPart, err)
			}
			return int64(n * float64(unit.multiplier)), nil
		}
	}

	// No recognized unit suffix: treat the whole value as a plain integer
	// number of bytes.
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unrecognized byte size format: %q", s)
	}
	return n, nil
}
