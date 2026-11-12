// Package config loads and validates pt-boost's YAML configuration: the
// torrentfs-go connection details and the per-site scan/pool/eviction
// parameters described in docs/design-overview.md.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// defaultPoolSize is used whenever a site config omits pool_size (or sets it
// to 0), per the "热种池 (Hot Torrent Pool)" glossary entry: capacity has a
// default of 10, overridable per site.
const defaultPoolSize = 10

// defaultStatusListenAddr is used whenever the optional status.listen_addr
// field is omitted. It deliberately differs from torrentfs-go's typical
// 8080 default (see design-overview.md's example base_url) to avoid a
// same-host port clash when both run outside Docker's per-container
// network namespace.
const defaultStatusListenAddr = ":8081"

// Config is the root of pt-boost's configuration: how to reach torrentfs-go,
// the list of PT sites to run a scheduler for, and where to serve the
// read-only GET /status endpoint (docs/design-overview.md §7 / issue #7).
type Config struct {
	TorrentFS TorrentFSConfig
	Sites     []SiteConfig
	Status    StatusConfig
}

// StatusConfig configures the GET /status HTTP endpoint. There is
// deliberately no Prometheus /metrics equivalent in v1 (issue #7 rules it
// out explicitly).
type StatusConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

// TorrentFSConfig holds how to reach the torrentfs-go instance pt-boost
// drives via its REST API (see ADR 0001). pt-boost never reads or validates
// torrentfs-go's own deployment-time configuration beyond these fields.
type TorrentFSConfig struct {
	BaseURL  string `yaml:"base_url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// SiteConfig holds one PT site's adapter credentials and the scheduler
// parameters for its hot-torrent pool.
type SiteConfig struct {
	Name              string
	APIKey            string
	BaseURL           string
	Category          string
	PoolSize          int
	SeedersMin        int
	LeechersMin       int
	ScanInterval      Duration
	MaxScanPages      int
	PageSize          int
	RequestInterval   Duration
	TargetShareBytes  ByteSize
	ColdWindow        Duration
	ColdRateThreshold Rate
}

// rawConfig and rawSiteConfig mirror the YAML shape with plain string
// fields for anything that needs custom parsing (durations, byte sizes,
// rates). Decoding into these first lets every conversion error be wrapped
// with the exact dotted/indexed field path it came from.
type rawConfig struct {
	TorrentFS TorrentFSConfig `yaml:"torrentfs"`
	Sites     []rawSiteConfig `yaml:"sites"`
	Status    StatusConfig    `yaml:"status"`
}

type rawSiteConfig struct {
	Name              string `yaml:"name"`
	APIKey            string `yaml:"api_key"`
	BaseURL           string `yaml:"base_url"`
	Category          string `yaml:"category"`
	PoolSize          int    `yaml:"pool_size"`
	SeedersMin        int    `yaml:"seeders_min"`
	LeechersMin       int    `yaml:"leechers_min"`
	ScanInterval      string `yaml:"scan_interval"`
	MaxScanPages      int    `yaml:"max_scan_pages"`
	PageSize          int    `yaml:"page_size"`
	RequestInterval   string `yaml:"request_interval"`
	TargetShareBytes  string `yaml:"target_share_bytes"`
	ColdWindow        string `yaml:"cold_window"`
	ColdRateThreshold string `yaml:"cold_rate_threshold"`
}

// envVarPattern matches "${NAME}" placeholders so secrets such as
// sites[].api_key can be kept out of the YAML file (and out of git) and
// supplied via the environment or a sibling .env file instead (see
// cmd/pt-boost/main.go). Only the braced form is recognized, so a bare "$"
// in, say, a URL or torrent name is never mistaken for a placeholder.
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces every "${NAME}" placeholder in data with the value of
// the environment variable NAME. A placeholder whose variable is unset is
// left verbatim, so Parse's required-field validation still surfaces a
// useful "missing or empty" error instead of silently swallowing it.
func expandEnv(data []byte) []byte {
	return envVarPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		name := string(match[2 : len(match)-1])
		if v, ok := os.LookupEnv(name); ok {
			return []byte(v)
		}
		return match
	})
}

// Load reads the YAML file at path, expands any "${NAME}" environment
// variable placeholders it contains, and parses the result into a Config.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %q: %w", path, err)
	}
	return Parse(expandEnv(data))
}

// Parse decodes YAML bytes into a validated Config. It applies defaults
// (pool_size defaults to 10 when omitted) and returns an error naming every
// missing or malformed field it finds, rather than failing on the first one.
func Parse(data []byte) (*Config, error) {
	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing config YAML: %w", err)
	}

	var errs []error
	requireNonEmpty(&errs, "torrentfs.base_url", raw.TorrentFS.BaseURL)
	requireNonEmpty(&errs, "torrentfs.username", raw.TorrentFS.Username)
	requireNonEmpty(&errs, "torrentfs.password", raw.TorrentFS.Password)

	sites := make([]SiteConfig, len(raw.Sites))
	for i, rs := range raw.Sites {
		site, siteErrs := convertSite(i, rs)
		errs = append(errs, siteErrs...)
		sites[i] = site
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	status := raw.Status
	if status.ListenAddr == "" {
		status.ListenAddr = defaultStatusListenAddr
	}

	return &Config{TorrentFS: raw.TorrentFS, Sites: sites, Status: status}, nil
}

func convertSite(i int, rs rawSiteConfig) (SiteConfig, []error) {
	var errs []error
	path := func(field string) string { return fmt.Sprintf("sites[%d].%s", i, field) }

	requireNonEmpty(&errs, path("name"), rs.Name)
	requireNonEmpty(&errs, path("api_key"), rs.APIKey)
	requireNonEmpty(&errs, path("base_url"), rs.BaseURL)
	requireNonEmpty(&errs, path("category"), rs.Category)

	poolSize := rs.PoolSize
	if poolSize == 0 {
		poolSize = defaultPoolSize
	}

	site := SiteConfig{
		Name:        rs.Name,
		APIKey:      rs.APIKey,
		BaseURL:     rs.BaseURL,
		Category:    rs.Category,
		PoolSize:    poolSize,
		SeedersMin:  rs.SeedersMin,
		LeechersMin: rs.LeechersMin,

		MaxScanPages: rs.MaxScanPages,
		PageSize:     rs.PageSize,
	}

	site.ScanInterval = parseFieldOrAppend(&errs, path("scan_interval"), rs.ScanInterval, parseDuration)
	site.RequestInterval = parseFieldOrAppend(&errs, path("request_interval"), rs.RequestInterval, parseDuration)
	site.ColdWindow = parseFieldOrAppend(&errs, path("cold_window"), rs.ColdWindow, parseDuration)
	site.TargetShareBytes = parseFieldOrAppend(&errs, path("target_share_bytes"), rs.TargetShareBytes, parseByteSizeField)
	site.ColdRateThreshold = parseFieldOrAppend(&errs, path("cold_rate_threshold"), rs.ColdRateThreshold, parseRate)

	return site, errs
}

// parseFieldOrAppend runs parse on raw; on failure it appends a
// field-prefixed error to errs and returns the zero value, so convertSite
// can keep collecting every error in a site before returning.
func parseFieldOrAppend[T any](errs *[]error, field, raw string, parse func(string) (T, error)) T {
	value, err := parse(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", field, err))
		var zero T
		return zero
	}
	return value
}

func requireNonEmpty(errs *[]error, field, value string) {
	if value == "" {
		*errs = append(*errs, fmt.Errorf("%s: required field is missing or empty", field))
	}
}
