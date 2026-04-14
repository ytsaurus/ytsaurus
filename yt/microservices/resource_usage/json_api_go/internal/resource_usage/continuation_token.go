package resourceusage

import (
	"fmt"
	"strings"
)

type ContinuationToken struct {
	Features *ResourceUsageTableFeatures `json:"features,omitempty"`
	Account  string                      `json:"account"`
	Depth    int64                       `json:"depth"`
	Path     string                      `json:"path"`
	Type     string                      `json:"type,omitempty"`
}

func (ct *ContinuationToken) GetExpressionAndPlaceholderValues() (string, map[string]any) {
	keys := []string{"account", "depth", "path"}
	values := []any{
		ct.Account,
		ct.Depth,
		ct.Path,
	}

	if ct.Features != nil && ct.Features.TypeInKey > 0 {
		keys = append(keys, "type")
		values = append(values, ct.Type)
	}

	return fmt.Sprintf("(%s) > ({continuation_token_values})",
			strings.Join(keys, ", "),
		), map[string]any{
			"continuation_token_values": values,
		}
}
