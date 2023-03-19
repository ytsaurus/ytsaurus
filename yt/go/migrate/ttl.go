package migrate

import (
	"time"

	"go.ytsaurus.tech/library/go/ptr"
)

// RetentionConfig stores dynamic table retention policy.
type RetentionConfig struct {
	MinDataVersions *int
	MaxDataVersions *int
	MinDataTTL      *int
	MaxDataTTL      *int
}

// DeleteDataAfterTTL is retention config that would remove versions
// after specified ttl.
func DeleteDataAfterTTL(ttl time.Duration) RetentionConfig {
	return RetentionConfig{
		MinDataVersions: ptr.Int(0),
		MaxDataVersions: ptr.Int(1),
		MinDataTTL:      ptr.Int(0),
		MaxDataTTL:      ptr.Int(int(ttl.Milliseconds())),
	}
}

func (c RetentionConfig) FillAttrs(attrs map[string]interface{}) {
	if c.MinDataVersions != nil {
		attrs["min_data_versions"] = *c.MinDataVersions
	}

	if c.MaxDataVersions != nil {
		attrs["max_data_versions"] = *c.MaxDataVersions
	}

	if c.MinDataTTL != nil {
		attrs["min_data_ttl"] = *c.MinDataTTL
	}

	if c.MaxDataTTL != nil {
		attrs["max_data_ttl"] = *c.MaxDataTTL
	}
}
