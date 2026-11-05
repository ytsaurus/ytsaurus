package monitoring

import (
	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/library/go/core/metrics/solomon"
)

func RegisterHTTPMetrics(registry *solomon.Registry) chi.Router {
	return chi.NewRouter()
}
