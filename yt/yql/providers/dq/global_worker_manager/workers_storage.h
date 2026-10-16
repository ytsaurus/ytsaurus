#pragma once

#include "worker_filter.h"

#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/worker_info.h>

#include <yt/yql/providers/dq/scheduler/dq_scheduler.h>

namespace NYql {

class TWorkersStorage {
public:
    TWorkersStorage(ui32 nodeId, TSensorsGroupPtr metrics, TSensorsGroupPtr workerMetrics);

    void Clear();

    TList<NDqs::TWorkerInfo::TPtr> GetList();

    size_t Capacity() const { return GlobalResources.GetCapacity(); }
    size_t FreeSlots() const { return Max<i64>(GlobalResources.GetCapacity() - GlobalResources.GetRunningRequests(), 0); }

    std::tuple<NDqs::TWorkerInfo::TPtr, bool> CreateOrUpdate(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request);

    void CheckZombie(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request);

    void CleanUp(TInstant now, TDuration duration);

    void DropWorker(TInstant now, TGUID workerId);

    void FreeWorker(TInstant now, const NDqs::TWorkerInfo::TPtr& worker);

    TVector<NDqs::TWorkerInfo::TPtr> TryAllocate(const NDq::IScheduler::TWaitInfo& waitInfo) noexcept;

    void IsReady(const TVector<NDqs::TWorkerInfo::TFileResource>& resources, THashMap<TString, std::pair<ui32, ui32>>& clusterMap);

    void ClusterStatus(Yql::DqsProto::ClusterStatusResponse* r) const;

    void UpdateResourceUseTime(TDuration duration, const THashSet<TString>& ids);

    bool HasResource(const TString& id) const;

    void AddResource(const TString& id, Yql::DqsProto::TFile::EFileType type, const TString& name, i64 size);

    void UpdateMetrics();

    void Visit(const std::function<void(const NDqs::TWorkerInfo::TPtr& workerInfo)>& f);

private:

    // Max total file lookups across both SearchFreeList passes per TryAllocate call.
    // Each outer iteration (worker) costs M * K file lookups in the no-match worst case.
    // Effective outer iterations per pass = limit / (M * K_avg).
    //
    // At 3GHz, ~50ns per hash lookup:
    //   1M lookups ≈ 50ms per TryAllocate call.
    //
    // Current cluster: N=500, M=100, K=100 → full scan = 10M lookups > limit →
    //   maxOuterItersPerPass = 1M / (100*100) = 100 outer iters scanned per pass (20% of 500 workers).
    // Target cluster: N=10000, M=100, K=100 → maxOuterItersPerPass = 100 → 1% of 10000 workers scanned.
    //   This aggressively limits search but prevents GWM from blocking for seconds.
    //
    // The real fix for large N/M/K is an inverted file→workers index (see architecture notes).
    // 0 = unlimited (for tests / manual override).
    size_t SearchComplexityLimit = 1'000'000;

    const i32 MaxDownloadsPerFile = 20;
    ui32 NodeId;
    THashMap<TString, NDqs::TInflightLimiter::TPtr> InflightLimiters;
    THashMap<TGUID, NDqs::TWorkerInfo::TPtr> Workers;

    using TFreeList = std::set<NDqs::TWorkerInfo::TPtr, NDqs::TWorkerInfoPtrComparator>;
    TFreeList FreeList;

    TSensorsGroupPtr Metrics;
    TSensorsGroupPtr WorkerMetrics;
    TSensorsGroupPtr ResourceCounters;
    TSensorsGroupPtr ResourceDownloadCounters;
    NMonitoring::THistogramPtr ResourceWaitTime;
    NMonitoring::TDynamicCounters::TCounterPtr WorkersSize;
    NMonitoring::TDynamicCounters::TCounterPtr FreeListSize;
    NMonitoring::TDynamicCounters::TCounterPtr SearchIterationsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr TryAllocateOverloadedCounter;
    TString StartTime = ToString(TInstant::Now());
    TVector<std::pair<TGUID, TString>> NodeIds;

    THashMap<TString, TResourceStat> Uploaded;

    // Inverted index: fileId -> workers that have this file.
    // Maintained in CreateOrUpdate, DropWorker, CleanUp, Clear.
    // Used by SearchFreeListWithIndex to find candidate workers in O(K * |workers_per_file|)
    // instead of the O(N * M * K) full FreeList scan.
    THashMap<TString, THashSet<NDqs::TWorkerInfo::TPtr>> FileToWorkers;

    NDqs::TGlobalResources GlobalResources;

    TFreeList::iterator ProcessMatched(
        TWorkersStorage::TFreeList::iterator it,
        TVector<NDqs::TWorkerInfo::TPtr>& result,
        TFreeList* freeList,
        THashMap<NDqs::TWorkerInfo::TPtr, int>& tasksPerWorker,
        THashMap<i32, TWorkerFilter>& tasksToAllocate,
        int taskId);

    // Returns number of outer (per-worker) iterations performed.
    // maxOuterIters==0 means unlimited.
    size_t SearchFreeList(
        TVector<NDqs::TWorkerInfo::TPtr>& result,
        TFreeList* freeList,
        const i32 maxTasksPerWorker,
        THashMap<NDqs::TWorkerInfo::TPtr, int>& tasksPerWorker,
        THashMap<i32, TWorkerFilter>& tasksToAllocate,
        THashMap<TString, THashSet<int>>& waitingResources,
        TWorkerFilter::EMatchStatus matchMode,
        size_t maxOuterIters = 0);

    // Fast path: for each task with file filters, find candidate workers via
    // FileToWorkers inverted index (intersection of per-file worker sets) instead of
    // scanning the whole FreeList. Complexity: O(M * K * |smallest_file_set|) instead
    // of O(N * M * K). Tasks without file filters or with no candidates are left in
    // tasksToAllocate for the fallback SearchFreeList pass.
    // Returns total ops performed (for metrics).
    size_t SearchFreeListWithIndex(
        TVector<NDqs::TWorkerInfo::TPtr>& result,
        TFreeList* freeList,
        const i32 maxTasksPerWorker,
        THashMap<NDqs::TWorkerInfo::TPtr, int>& tasksPerWorker,
        THashMap<i32, TWorkerFilter>& tasksToAllocate,
        THashMap<TString, THashSet<int>>& waitingResources);
};

} // namespace NYql
