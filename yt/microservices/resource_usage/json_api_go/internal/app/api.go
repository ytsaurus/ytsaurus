package app

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/atomic"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/access"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

type API struct {
	l             log.Structured
	ready         atomic.Bool
	resourceUsage *resourceusage.ResourceUsage
	accessChecker *access.AccessChecker
}

func NewAPI(l log.Structured, resourceUsageConfig *resourceusage.Config, accessCheckerConfig *access.Config) *API {
	resourceUsageL := log.With(l.Logger(), log.String("component", "resource_usage")).Structured()

	accessChecker, err := access.NewAccessChecker(l, accessCheckerConfig)
	if err != nil {
		l.Fatal("failed to create access checker", log.Error(err))
	}
	resourceUsage, err := resourceusage.NewResourceUsage(resourceUsageConfig, resourceUsageL)
	if err != nil {
		l.Fatal("failed to create resource usage", log.Error(err))
	}

	return &API{
		l:             l,
		resourceUsage: resourceUsage,
		accessChecker: accessChecker,
	}
}

func (a *API) StartServingClusters(ctx context.Context) {
	a.resourceUsage.StartServingClusters(ctx)
}

func (a *API) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/ready", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(ReadyResponse{Ready: true})
		})
	})

	r.Route("/served-clusters", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Post("/", a.servedClustersHandler)
	})

	r.Route("/list-timestamps", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Post("/", a.listTimestampsHandler)
	})

	r.Route("/get-resource-usage", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Use(basePathCleanupMiddleware)
		r.Use(a.accessChecker.UserAuthMiddleware)
		r.Post("/", a.resourceUsageHandler)
	})

	r.Route("/get-children-and-resource-usage", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Use(basePathDefaultMiddleware)
		r.Use(a.accessChecker.UserAuthMiddleware)
		r.Post("/", a.resourceUsageHandler)
	})

	r.Route("/get-resource-usage-diff", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Use(basePathCleanupMiddleware)
		r.Use(a.accessChecker.UserAuthMiddleware)
		r.Post("/", a.resourceUsageDiffHandler)
	})

	r.Route("/get-children-and-resource-usage-diff", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Use(basePathDefaultMiddleware)
		r.Use(a.accessChecker.UserAuthMiddleware)
		r.Post("/", a.resourceUsageDiffHandler)
	})

	r.Route("/get-versioned-resource-usage", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Use(a.accessChecker.UserAuthMiddleware)
		r.Post("/", a.versionedResourceUsageHandler)
	})

	r.Route("/status", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Post("/", a.statusHandler)
	})

	r.Route("/whoami", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Use(a.accessChecker.UserAuthMiddleware)
		r.Post("/", a.whoamiHandler)
	})

	return r
}

func (a *API) whoamiHandler(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value(access.AuthInfoKey).(access.AuthInfo)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(WhoamiResponse{Error: "failed to get user"})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(WhoamiResponse{User: user.Login, IsService: user.IsService})
}

func (a *API) statusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	clusterNames, err := a.resourceUsage.GetServedClusters()
	if err != nil {
		ctxlog.Error(ctx, a.l.Logger(), "failed to get served clusters", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(StatusResponse{Error: err.Error()})
		return
	}

	response := StatusResponse{
		Stats: map[string]AtomsStatus{},
	}
	for _, clusterName := range clusterNames {
		cluster, err := a.resourceUsage.GetCluster(ctx, clusterName)
		if err != nil {
			ctxlog.Error(ctx, a.l.Logger(), "failed to get cluster", log.String("cluster", clusterName), log.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(StatusResponse{Error: err.Error()})
			return
		}
		tableReaderStatus, err := cluster.GetClusterStatus(ctx)
		if err != nil {
			ctxlog.Error(ctx, a.l.Logger(), "failed to get cluster status", log.String("cluster", clusterName), log.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(StatusResponse{Error: err.Error()})
			return
		}

		response.Stats[clusterName] = AtomsStatus{
			TableReader: tableReaderStatus,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(StatusResponse{Stats: response.Stats})
}

func (a *API) versionedResourceUsageHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := GetVersionedResourceUsageRequest{
		TimestampSelector: resourceusage.TimestampSelector{
			Timestamp:               time.Now().Unix(),
			TimestampRoundingPolicy: "closest",
		},
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(GetVersionedResourceUsageResponse{Error: err.Error()})
		return
	}

	cluster, err := a.resourceUsage.GetCluster(ctx, req.Cluster)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetVersionedResourceUsageResponse{Error: err.Error()})
		return
	}

	items, available, err := cluster.GetVersionedResourceUsage(
		ctx,
		req.Timestamp,
		req.TimestampRoundingPolicy,
		req.Account,
		req.Path,
		req.Type,
	)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetVersionedResourceUsageResponse{Error: err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(GetVersionedResourceUsageResponse{
		Transactions: items,
		Available:    available,
	})
}

func (a *API) resourceUsageHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user, ok := ctx.Value(access.AuthInfoKey).(access.AuthInfo)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(WhoamiResponse{Error: "failed to get user"})
		return
	}
	req := GetResourceUsageRequest{
		Timestamp:               time.Now().Unix(),
		TimestampRoundingPolicy: "closest",
		Page: &resourceusage.PageSelector{
			Index: 0,
			Size:  20,
		},
		RowFilter: &resourceusage.Filter{
			ExcludeMapNodes: true,
		},
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}

	cluster, err := a.resourceUsage.GetCluster(ctx, req.Cluster)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}

	resourceUsage, err := cluster.GetResourceUsage(
		ctx,
		resourceusage.ResourceUsageInput{
			Account:                 req.Account,
			Timestamp:               req.Timestamp,
			TimestampRoundingPolicy: req.TimestampRoundingPolicy,
			RowFilter:               req.RowFilter,
			SortOrders:              req.SortOrders,
			PageSelector:            req.Page,
		},
	)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}

	err = a.accessChecker.CheckAccess(ctx, req.Cluster, user.Login, resourceUsage.Items)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{
		SnapshotTimestamp: resourceUsage.SnapshotTimestamp,
		Fields:            resourceUsage.Fields,
		RowCount:          resourceUsage.RowCount,
		Items:             resourceUsage.Items,
		Mediums:           resourceUsage.Mediums,
		ContinuationToken: resourceUsage.ContinuationToken,
	})
}

func (a *API) resourceUsageDiffHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user, ok := ctx.Value(access.AuthInfoKey).(access.AuthInfo)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(WhoamiResponse{Error: "failed to get user"})
		return
	}
	req := GetResourceUsageDiffRequest{
		Page: &resourceusage.PageSelector{
			Index: 0,
			Size:  20,
		},
		RowFilter: &resourceusage.Filter{
			ExcludeMapNodes: true,
		},
		Timestamps: &DiffSnapshotSelector{
			Newer: &resourceusage.TimestampSelector{
				Timestamp:               time.Now().Unix(),
				TimestampRoundingPolicy: "closest",
			},
			Older: &resourceusage.TimestampSelector{
				Timestamp:               time.Now().Add(-24 * time.Hour).Unix(),
				TimestampRoundingPolicy: "backward",
			},
		},
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}
	cluster, err := a.resourceUsage.GetCluster(ctx, req.Cluster)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}

	resourceUsageDiff, err := cluster.GetResourceUsageDiff(
		ctx,
		resourceusage.ResourceUsageDiffInput{
			Account:                req.Account,
			OlderTimestampSelector: req.Timestamps.Older,
			NewerTimestampSelector: req.Timestamps.Newer,
			RowFilter:              req.RowFilter,
			SortOrders:             req.SortOrders,
			PageSelector:           req.Page,
		},
	)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetResourceUsageDiffResponse{Error: err.Error()})
		return
	}

	err = a.accessChecker.CheckAccess(ctx, req.Cluster, user.Login, resourceUsageDiff.Items)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetResourceUsageResponse{Error: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(GetResourceUsageDiffResponse{
		SnapshotTimestamps: DiffSnapshotSelectorResponse{
			Older: resourceUsageDiff.OldSelectedSnapshot,
			Newer: resourceUsageDiff.NewSelectedSnapshot,
		},
		Fields:            resourceUsageDiff.Fields,
		RowCount:          resourceUsageDiff.RowCount,
		Items:             resourceUsageDiff.Items,
		Mediums:           resourceUsageDiff.Mediums,
		ContinuationToken: resourceUsageDiff.ContinuationToken,
	})
}

type ReadyResponse struct {
	Ready bool `json:"ready"`
}

func (a *API) servedClustersHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")
	servedClusters, err := a.resourceUsage.GetServedClusters()
	if err != nil {
		ctxlog.Error(ctx, a.l.Logger(), "failed to get served clusters", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ServedClustersResponse{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ServedClustersResponse{
		Clusters: servedClusters,
	})
}

func (a *API) SetReady() {
	a.ready.Store(true)
	a.l.Info("api is ready")
}

func (a *API) listTimestampsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req := ListTimestampsRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	cluster, err := a.resourceUsage.GetCluster(ctx, req.Cluster)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListTimestampsResponse{Error: err.Error()})
		return
	}

	snapshotTimestamps, err := cluster.GetListTimestamps(req.From, req.To)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListTimestampsResponse{Error: err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ListTimestampsResponse{
		SnapshotTimestamps: snapshotTimestamps,
	})
}

func (a *API) RegisterMetrics(r metrics.Registry) {}
