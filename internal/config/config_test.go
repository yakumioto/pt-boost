package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

const minimalValidYAML = `
torrentfs:
  base_url: http://torrentfs:8080
  username: admin
  password: secret

sites:
  - name: mteam
    api_key: abc123
    base_url: https://api.m-team.cc/api
    category: boost-mteam
    seeders_min: 10
    leechers_min: 50
    scan_interval: 10m
    max_scan_pages: 3
    page_size: 100
    request_interval: 3s
    target_share_bytes: 256MiB
    cold_window: 20m
    cold_rate_threshold: 50KB/s
`

func TestParse_MinimalValidYAML(t *testing.T) {
	cfg, err := Parse([]byte(minimalValidYAML))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if cfg.TorrentFS.BaseURL != "http://torrentfs:8080" {
		t.Errorf("TorrentFS.BaseURL = %q", cfg.TorrentFS.BaseURL)
	}
	if cfg.TorrentFS.Username != "admin" || cfg.TorrentFS.Password != "secret" {
		t.Errorf("TorrentFS credentials not parsed correctly: %+v", cfg.TorrentFS)
	}

	if len(cfg.Sites) != 1 {
		t.Fatalf("expected 1 site, got %d", len(cfg.Sites))
	}
	site := cfg.Sites[0]
	if site.Name != "mteam" || site.APIKey != "abc123" || site.Category != "boost-mteam" {
		t.Errorf("site identity fields wrong: %+v", site)
	}
	if site.SeedersMin != 10 || site.LeechersMin != 50 {
		t.Errorf("threshold fields wrong: seeders_min=%d leechers_min=%d", site.SeedersMin, site.LeechersMin)
	}
	if site.MaxScanPages != 3 || site.PageSize != 100 {
		t.Errorf("paging fields wrong: max_scan_pages=%d page_size=%d", site.MaxScanPages, site.PageSize)
	}
	if cfg.Status.ListenAddr != defaultStatusListenAddr {
		t.Errorf("Status.ListenAddr = %q, want default %q (status section omitted)", cfg.Status.ListenAddr, defaultStatusListenAddr)
	}
}

func TestParse_StatusListenAddrExplicitOverridesDefault(t *testing.T) {
	yaml := minimalValidYAML + "\nstatus:\n  listen_addr: \"127.0.0.1:9100\"\n"
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if cfg.Status.ListenAddr != "127.0.0.1:9100" {
		t.Errorf("Status.ListenAddr = %q, want 127.0.0.1:9100", cfg.Status.ListenAddr)
	}
}

func TestParse_PoolSizeDefaultsTo10WhenOmitted(t *testing.T) {
	cfg, err := Parse([]byte(minimalValidYAML))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if cfg.Sites[0].PoolSize != 10 {
		t.Errorf("PoolSize = %d, want default 10", cfg.Sites[0].PoolSize)
	}
}

func TestParse_PoolSizeExplicitOverridesDefault(t *testing.T) {
	yaml := strings.Replace(minimalValidYAML, "seeders_min: 10", "pool_size: 5\n    seeders_min: 10", 1)
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if cfg.Sites[0].PoolSize != 5 {
		t.Errorf("PoolSize = %d, want explicit 5", cfg.Sites[0].PoolSize)
	}
}

func TestParse_DurationFieldsParseToTimeDuration(t *testing.T) {
	cfg, err := Parse([]byte(minimalValidYAML))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	site := cfg.Sites[0]
	if site.ScanInterval.Duration != 10*time.Minute {
		t.Errorf("ScanInterval = %v, want 10m", site.ScanInterval.Duration)
	}
	if site.RequestInterval.Duration != 3*time.Second {
		t.Errorf("RequestInterval = %v, want 3s", site.RequestInterval.Duration)
	}
	if site.ColdWindow.Duration != 20*time.Minute {
		t.Errorf("ColdWindow = %v, want 20m", site.ColdWindow.Duration)
	}
}

func TestParse_ByteSizeFieldParsesMiB(t *testing.T) {
	cfg, err := Parse([]byte(minimalValidYAML))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	const want = 256 * 1024 * 1024
	if cfg.Sites[0].TargetShareBytes.Bytes != want {
		t.Errorf("TargetShareBytes = %d, want %d", cfg.Sites[0].TargetShareBytes.Bytes, want)
	}
}

func TestParse_RateFieldParsesKBPerSecond(t *testing.T) {
	cfg, err := Parse([]byte(minimalValidYAML))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	const want = 50 * 1000
	if cfg.Sites[0].ColdRateThreshold.BytesPerSecond != want {
		t.Errorf("ColdRateThreshold = %d, want %d", cfg.Sites[0].ColdRateThreshold.BytesPerSecond, want)
	}
}

func TestParse_MissingTorrentFSBaseURLReturnsFieldNamedError(t *testing.T) {
	bad := strings.Replace(minimalValidYAML, "base_url: http://torrentfs:8080", "base_url: \"\"", 1)
	_, err := Parse([]byte(bad))
	if err == nil {
		t.Fatal("expected error for missing torrentfs.base_url, got nil")
	}
	if !strings.Contains(err.Error(), "torrentfs.base_url") {
		t.Errorf("error does not name the missing field: %v", err)
	}
}

func TestParse_MissingSiteNameReturnsFieldNamedError(t *testing.T) {
	bad := strings.Replace(minimalValidYAML, "name: mteam", "name: \"\"", 1)
	_, err := Parse([]byte(bad))
	if err == nil {
		t.Fatal("expected error for missing sites[0].name, got nil")
	}
	if !strings.Contains(err.Error(), "sites[0].name") {
		t.Errorf("error does not name the missing field: %v", err)
	}
}

func TestParse_InvalidDurationReturnsFieldNamedError(t *testing.T) {
	bad := strings.Replace(minimalValidYAML, "scan_interval: 10m", "scan_interval: not-a-duration", 1)
	_, err := Parse([]byte(bad))
	if err == nil {
		t.Fatal("expected error for invalid scan_interval, got nil")
	}
	if !strings.Contains(err.Error(), "scan_interval") {
		t.Errorf("error does not name the offending field: %v", err)
	}
}

func TestParse_InvalidByteSizeReturnsFieldNamedError(t *testing.T) {
	bad := strings.Replace(minimalValidYAML, "target_share_bytes: 256MiB", "target_share_bytes: not-a-size", 1)
	_, err := Parse([]byte(bad))
	if err == nil {
		t.Fatal("expected error for invalid target_share_bytes, got nil")
	}
	if !strings.Contains(err.Error(), "target_share_bytes") {
		t.Errorf("error does not name the offending field: %v", err)
	}
}

func TestParse_MalformedYAMLSyntaxReturnsError(t *testing.T) {
	_, err := Parse([]byte("not: [valid: yaml"))
	if err == nil {
		t.Fatal("expected error for malformed YAML syntax, got nil")
	}
}

func TestParse_MultipleSitesAllParsed(t *testing.T) {
	yaml := minimalValidYAML + `
  - name: othersite
    api_key: xyz789
    base_url: https://example.invalid/api
    category: boost-other
    seeders_min: 5
    leechers_min: 20
    scan_interval: 5m
    max_scan_pages: 2
    page_size: 50
    request_interval: 1s
    target_share_bytes: 128MiB
    cold_window: 10m
    cold_rate_threshold: 20KB/s
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(cfg.Sites) != 2 {
		t.Fatalf("expected 2 sites, got %d", len(cfg.Sites))
	}
	if cfg.Sites[1].Name != "othersite" || cfg.Sites[1].Category != "boost-other" {
		t.Errorf("second site not parsed correctly: %+v", cfg.Sites[1])
	}
}

func TestLoad_ReadsFileAndParses(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.yaml"
	if err := os.WriteFile(path, []byte(minimalValidYAML), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Sites[0].Name != "mteam" {
		t.Errorf("Load did not parse file content correctly: %+v", cfg.Sites[0])
	}
}

func TestLoad_ExpandsEnvVarPlaceholderInFile(t *testing.T) {
	t.Setenv("PT_BOOST_TEST_API_KEY", "secret-from-env")

	yamlWithPlaceholder := strings.Replace(minimalValidYAML, "api_key: abc123", "api_key: ${PT_BOOST_TEST_API_KEY}", 1)
	dir := t.TempDir()
	path := dir + "/config.yaml"
	if err := os.WriteFile(path, []byte(yamlWithPlaceholder), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Sites[0].APIKey != "secret-from-env" {
		t.Errorf("APIKey = %q, want expanded env var value", cfg.Sites[0].APIKey)
	}
}

func TestLoad_UnsetEnvVarPlaceholderLeftVerbatim(t *testing.T) {
	os.Unsetenv("PT_BOOST_TEST_UNSET_VAR")

	yamlWithPlaceholder := strings.Replace(minimalValidYAML, "api_key: abc123", "api_key: ${PT_BOOST_TEST_UNSET_VAR}", 1)
	dir := t.TempDir()
	path := dir + "/config.yaml"
	if err := os.WriteFile(path, []byte(yamlWithPlaceholder), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Sites[0].APIKey != "${PT_BOOST_TEST_UNSET_VAR}" {
		t.Errorf("APIKey = %q, want the placeholder left verbatim when the env var is unset", cfg.Sites[0].APIKey)
	}
}

func TestLoad_MissingFileReturnsError(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}
