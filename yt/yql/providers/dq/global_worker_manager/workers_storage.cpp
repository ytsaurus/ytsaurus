#include "workers_storage.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

#include <util/generic/scope.h>

#include <algorithm>

namespace NYql {

using namespace NDqs;
using namespace NMonitoring;

using EFileType = Yql::DqsProto::TFile::EFileType;
using TFileResource = Yql::DqsProto::TFile;

TWorkersStorage::TWorkersStorage(
    ui32 nodeId,
    TSensorsGroupPtr metrics,
    TSensorsGroupPtr workerMetrics,
    bool trackSlotsSnapshot)
    : NodeId(nodeId)
    , Metrics(std::move(metrics))
    , WorkerMetrics(std::move(workerMetrics))
    , ResourceCounters(Metrics->GetSubgroup("component", "ResourceCounters"))
    , ResourceDownloadCounters(Metrics->GetSubgroup("component", "ResourceDownloadCounters"))
    , ResourceWaitTime(Metrics
        ->GetSubgroup("component", "Resource")
        ->GetHistogram("WaitTime", ExponentialHistogram(10, 2, 1000)))
    , WorkersSize(nullptr)
    , FreeListSize(nullptr)
    , GlobalResources(Metrics)
    , TrackSlotsSnapshot(trackSlotsSnapshot)
{
    NodeIds.resize(65536);
}

void TWorkersStorage::Clear()
{
    SlotsSnapshot = {};
    Workers.clear();
    FreeList.clear();
    FileToWorkers.clear();
    WorkersSize = nullptr;
    FreeListSize = nullptr;
    Metrics->RemoveSubgroup("component", "lists");
}

TList<TWorkerInfo::TPtr> TWorkersStorage::GetList() {
    TList<TWorkerInfo::TPtr> workers;

    for (const auto& [_, workerInfo] : Workers) {
        workers.push_back(workerInfo);
    }

    return workers;
}

const TWorkersStorage::TSlotsSnapshot& TWorkersStorage::GetSlotsSnapshot() const
{
    return SlotsSnapshot;
}

TWorkersStorage::TWorkerSlots TWorkersStorage::GetSchedulableSlots(
    const TWorkerInfo::TPtr& workerInfo) const
{
    if (!TrackSlotsSnapshot || workerInfo->IsDead || workerInfo->Stopping) {
        return {};
    }
    const ui64 capacity = workerInfo->Capacity;
    const ui64 running = std::clamp<i64>(workerInfo->RunningRequests, 0, workerInfo->Capacity);
    return {
        .Capacity = capacity,
        .Free = capacity - running,
    };
}

void TWorkersStorage::UpdateSlotsSnapshot(
    const TWorkerInfo::TPtr& workerInfo,
    const TWorkerSlots& before)
{
    if (!TrackSlotsSnapshot) {
        return;
    }

    const auto after = GetSchedulableSlots(workerInfo);
    Y_ABORT_UNLESS(SlotsSnapshot.Capacity >= before.Capacity);
    Y_ABORT_UNLESS(SlotsSnapshot.Free >= before.Free);
    SlotsSnapshot.Capacity -= before.Capacity;
    SlotsSnapshot.Free -= before.Free;
    SlotsSnapshot.Capacity += after.Capacity;
    SlotsSnapshot.Free += after.Free;

    auto& cluster = SlotsSnapshot.Clusters[workerInfo->ClusterName];
    Y_ABORT_UNLESS(cluster.Capacity >= before.Capacity);
    Y_ABORT_UNLESS(cluster.Free >= before.Free);
    cluster.Capacity -= before.Capacity;
    cluster.Free -= before.Free;
    cluster.Capacity += after.Capacity;
    cluster.Free += after.Free;
    if (cluster.Capacity == 0) {
        Y_ABORT_UNLESS(cluster.Free == 0);
        SlotsSnapshot.Clusters.erase(workerInfo->ClusterName);
    }
}

void TWorkersStorage::SetStopping(const TWorkerInfo::TPtr& workerInfo, bool stopping)
{
    if (workerInfo->Stopping != stopping) {
        const auto before = GetSchedulableSlots(workerInfo);
        workerInfo->Stopping = stopping;
        UpdateSlotsSnapshot(workerInfo, before);
    }
}

static void AddWorkerToFileIndex(
    THashMap<TString, THashSet<TWorkerInfo::TPtr>>& index,
    const TWorkerInfo::TPtr& worker)
{
    for (const auto& fileId : worker->GetResources()) {
        index[fileId].insert(worker);
    }
}

static void RemoveWorkerFromFileIndex(
    THashMap<TString, THashSet<TWorkerInfo::TPtr>>& index,
    const TWorkerInfo::TPtr& worker)
{
    for (const auto& fileId : worker->GetResources()) {
        auto it = index.find(fileId);
        if (it != index.end()) {
            it->second.erase(worker);
            if (it->second.empty()) {
                index.erase(it);
            }
        }
    }
}

std::tuple<TWorkerInfo::TPtr, bool> TWorkersStorage::CreateOrUpdate(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request) {
    auto& knownWorkers = request.GetKnownNodes();
    bool needResume = false;
    auto maybeWorker = Workers.find(workerId);
    TWorkerInfo::TPtr workerInfo;

    CheckZombie(nodeId, workerId, request);

    if (maybeWorker == Workers.end()) {
        for (auto node : knownWorkers) {
            if (node == NodeId) {
                YQL_CLOG(DEBUG, ProviderDq)
                    << "Resume " << GetGuidAsString(workerId)
                    << " " << request.GetRevision()
                    << " " << request.GetRunningWorkers()
                    << " " << request.GetEpoch();

                TInflightLimiter::TPtr limiter = InflightLimiters[request.GetClusterName()];
                if (!limiter) {
                    limiter = new NDqs::TInflightLimiter(MaxDownloadsPerFile);
                    InflightLimiters[request.GetClusterName()] = limiter;
                }

                workerInfo = MakeIntrusive<TWorkerInfo>(StartTime, nodeId, workerId, request, WorkerMetrics, limiter, GlobalResources);
                Workers[workerId] = workerInfo;
                FreeList.insert(workerInfo);
                AddWorkerToFileIndex(FileToWorkers, workerInfo);
                UpdateSlotsSnapshot(workerInfo, {});
                needResume = true;
                break;
            }
        }
    } else {
        workerInfo = maybeWorker->second;
        const auto slotsBeforeUpdate = GetSchedulableSlots(workerInfo);
        FreeList.erase(workerInfo);
        // Snapshot old file set before Update so we can diff for the index.
        THashSet<TString> oldFiles = workerInfo->GetResources();
        needResume = workerInfo->Update(request);
        // Diff old vs new and patch FileToWorkers.
        const THashSet<TString>& newFiles = workerInfo->GetResources();
        for (const auto& fileId : oldFiles) {
            if (!newFiles.contains(fileId)) {
                auto it = FileToWorkers.find(fileId);
                if (it != FileToWorkers.end()) {
                    it->second.erase(workerInfo);
                    if (it->second.empty()) {
                        FileToWorkers.erase(it);
                    }
                }
            }
        }
        for (const auto& fileId : newFiles) {
            if (!oldFiles.contains(fileId)) {
                FileToWorkers[fileId].insert(workerInfo);
            }
        }
        if (workerInfo->RunningRequests < workerInfo->Capacity) {
            FreeList.insert(workerInfo);
        }
        UpdateSlotsSnapshot(workerInfo, slotsBeforeUpdate);
    }

    return std::make_tuple(workerInfo, needResume);
}

void TWorkersStorage::CheckZombie(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request) {
    Y_ABORT_UNLESS(nodeId < NodeIds.size());

    if (request.GetStartTime().empty()) {
        request.SetStartTime(StartTime);
    }

    if (!NodeIds[nodeId].first) {
        NodeIds[nodeId] = std::make_pair(workerId, request.GetStartTime());
        return;
    }
    if (NodeIds[nodeId].first != workerId) {
        auto curStartTime = TInstant::ParseIso8601(request.GetStartTime());
        auto prevStartTime = TInstant::ParseIso8601(NodeIds[nodeId].second);

        if (prevStartTime < curStartTime) {
            // Ping from new node: replace old node, mark old node as dead
            const auto oldWorkerId = NodeIds[nodeId].first;
            YQL_CLOG(DEBUG, ProviderDq) << "Zombie worker " << GetGuidAsString(oldWorkerId) << " " << nodeId;
            NodeIds[nodeId] = std::make_pair(workerId, request.GetStartTime());
            auto maybeWorker = Workers.find(oldWorkerId);
            if (maybeWorker != Workers.end()) {
                const auto before = GetSchedulableSlots(maybeWorker->second);
                maybeWorker->second->OnDead();
                UpdateSlotsSnapshot(maybeWorker->second, before);
            }
        } else {
            // Ping from old node
            YQL_CLOG(DEBUG, ProviderDq) << "Zombie worker " << GetGuidAsString(workerId) << " " << nodeId;
            request.SetZombie(true);
        }
    }
}

void TWorkersStorage::CleanUp(TInstant now, TDuration duration) {
    TVector<TGUID> deadWorkers;
    TWorkerInfo::TDownloadList downloadList;
    for (const auto& [k, v] : Workers) {
        if (now - v->LastPingTime > duration) {
            RemoveWorkerFromFileIndex(FileToWorkers, v);
            FreeList.erase(v);

            deadWorkers.push_back(k);
            downloadList.insert(v->GetDownloadList().begin(), v->GetDownloadList().end());
            const auto before = GetSchedulableSlots(v);
            v->OnDead();
            UpdateSlotsSnapshot(v, before);
        }
    }

    for (auto workerId : deadWorkers) {
        YQL_CLOG(WARN, ProviderDq) << "Remove dead worker " << GetGuidAsString(workerId);
        Workers.erase(workerId);
    }

    // pass dead download list to live workers
    auto it = Workers.begin();
    for (ui32 i = 0; i < deadWorkers.size() && it != Workers.end(); ++i, ++it) {
        it->second->AddToDownloadList(downloadList);
    }

    THashSet<TString> dropOldUploaded;
    for (const auto& [k, v] : Uploaded) {
        if (now - v.LastSeenTime > TDuration::Hours(1)) {
            dropOldUploaded.insert(k);
        }
    }
    for (const auto& k : dropOldUploaded) {
        Uploaded.erase(k);
    }
}

void TWorkersStorage::DropWorker(TInstant now, TGUID workerId) {
    Y_UNUSED(now);
    auto it = Workers.find(workerId);
    if (it != Workers.end()) {
        RemoveWorkerFromFileIndex(FileToWorkers, it->second);
        FreeList.erase(it->second);
        const auto before = GetSchedulableSlots(it->second);
        it->second->OnDead();
        UpdateSlotsSnapshot(it->second, before);
        Workers.erase(it);
    }
}

void TWorkersStorage::FreeWorker(TInstant now, const TWorkerInfo::TPtr& worker) {
    Y_UNUSED(now);

    FreeList.erase(worker);
    const auto before = GetSchedulableSlots(worker);
    if (worker->Release()) {
        FreeList.insert(worker);
    }
    UpdateSlotsSnapshot(worker, before);
}

static ui64 GetClusterBudget(
    const TWorkerInfo::TPtr& workerInfo,
    const TWorkersStorage::TLargeSlotsBudgetByCluster* largeSlotsBudgetByCluster)
{
    if (!largeSlotsBudgetByCluster) {
        return std::numeric_limits<ui64>::max();
    }
    const auto it = largeSlotsBudgetByCluster->find(workerInfo->ClusterName);
    return it == largeSlotsBudgetByCluster->end()
        ? std::numeric_limits<ui64>::max()
        : it->second;
}

static void ConsumeClusterBudget(
    const TWorkerInfo::TPtr& workerInfo,
    ui64 slots,
    TWorkersStorage::TLargeSlotsBudgetByCluster* largeSlotsBudgetByCluster)
{
    if (!largeSlotsBudgetByCluster) {
        return;
    }
    const auto it = largeSlotsBudgetByCluster->find(workerInfo->ClusterName);
    if (it != largeSlotsBudgetByCluster->end()) {
        Y_ABORT_UNLESS(it->second >= slots);
        it->second -= slots;
    }
}

struct TWorkersStorage::TAllocationBudget
{
    TLargeSlotsBudgetByCluster SlotsByCluster;
    TLargeSlotsBudgetByCluster MandatorySlotsByCluster;

    bool CanAllocate(const TWorkerInfo::TPtr& workerInfo, const TWorkerFilter& filter) const
    {
        ui64 mandatorySlots = 0;
        if (!filter.GetClusterName()) {
            const auto mandatoryIt = MandatorySlotsByCluster.find(workerInfo->ClusterName);
            if (mandatoryIt != MandatorySlotsByCluster.end()) {
                mandatorySlots = mandatoryIt->second;
            }
        }
        return GetClusterBudget(workerInfo, &SlotsByCluster) > mandatorySlots;
    }

    void Consume(const TWorkerInfo::TPtr& workerInfo, const TWorkerFilter& filter)
    {
        ConsumeClusterBudget(workerInfo, 1, &SlotsByCluster);
        if (filter.GetClusterName()) {
            const auto mandatoryIt = MandatorySlotsByCluster.find(filter.GetClusterName());
            Y_ABORT_UNLESS(mandatoryIt != MandatorySlotsByCluster.end() && mandatoryIt->second != 0);
            --mandatoryIt->second;
        }
    }
};

ui64 TWorkersStorage::BuildLegacyBoundedFreeList(
    TFreeList& boundedList,
    size_t workersCount,
    i32 maxTasksPerWorker)
{
    ui64 slots = 0;
    for (auto it = FreeList.begin(); it != FreeList.end() && boundedList.size() < workersCount; ) {
        const auto& workerInfo = *it;
        if (workerInfo->IsDead || workerInfo->Capacity <= workerInfo->RunningRequests) {
            it = FreeList.erase(it);
            continue;
        }

        const ui64 freeSlots = workerInfo->Capacity - workerInfo->RunningRequests;
        if (freeSlots >= static_cast<ui64>(maxTasksPerWorker)) {
            boundedList.insert(workerInfo);
            slots += freeSlots;
            it = FreeList.erase(it);
        } else {
            ++it;
        }
    }
    return slots;
}

ui64 TWorkersStorage::BuildBudgetedBoundedFreeList(
    TFreeList& boundedList,
    size_t workersCount,
    i32 maxTasksPerWorker,
    const TAllocationBudget& budget,
    const TWorkerFilter* mandatoryHostFilter,
    const THashMap<i32, TWorkerFilter>& taskFilters)
{
    struct TCandidate {
        TWorkerInfo::TPtr Worker;
        ui64 Slots = 0;
        size_t Order = 0;
        bool RequiredForMandatoryTasks = false;
    };

    THashMap<TString, TVector<TCandidate>> candidatesByCluster;
    size_t order = 0;
    for (auto it = FreeList.begin(); it != FreeList.end(); ) {
        const auto& workerInfo = *it;
        if (workerInfo->IsDead || workerInfo->Capacity <= workerInfo->RunningRequests) {
            it = FreeList.erase(it);
            continue;
        }
        if (!workerInfo->Stopping &&
            GetClusterBudget(workerInfo, &budget.SlotsByCluster) != 0 &&
            (!mandatoryHostFilter || mandatoryHostFilter->MatchHost(workerInfo)))
        {
            candidatesByCluster[workerInfo->ClusterName].push_back({
                .Worker = workerInfo,
                .Slots = Min<ui64>(
                    workerInfo->Capacity - workerInfo->RunningRequests,
                    maxTasksPerWorker),
                .Order = order,
            });
        }
        ++order;
        ++it;
    }

    // Cluster capacity cannot replace a worker required by an individual host filter.
    // Bound this extra search just like allocation; a truncated scan proves nothing.
    size_t hostChecks = 0;
    size_t requiredWorkerCount = 0;
    for (const auto& [_, filter] : taskFilters) {
        TCandidate* unique = nullptr;
        bool complete = true;
        bool multiple = false;
        for (auto& [_, candidates] : candidatesByCluster) {
            for (auto& candidate : candidates) {
                if (SearchComplexityLimit && hostChecks >= SearchComplexityLimit) {
                    complete = false;
                    break;
                }
                ++hostChecks;
                if (filter.MatchHost(candidate.Worker)) {
                    if (unique) {
                        multiple = true;
                        break;
                    }
                    unique = &candidate;
                }
            }
            if (!complete || multiple) {
                break;
            }
        }
        if (!complete) {
            break;
        }
        if (unique && !multiple && !unique->RequiredForMandatoryTasks) {
            unique->RequiredForMandatoryTasks = true;
            if (++requiredWorkerCount > workersCount) {
                return 0;
            }
        }
    }

    const auto compareCandidates = [] (const TCandidate& lhs, const TCandidate& rhs) {
        if (lhs.RequiredForMandatoryTasks != rhs.RequiredForMandatoryTasks) {
            return lhs.RequiredForMandatoryTasks;
        }
        return lhs.Slots != rhs.Slots
            ? lhs.Slots > rhs.Slots
            : lhs.Order < rhs.Order;
    };

    TVector<TCandidate> marginalCandidates;
    auto remainingMandatorySlots = budget.MandatorySlotsByCluster;
    for (auto& [clusterName, clusterCandidates] : candidatesByCluster) {
        const size_t clusterSelectedCount = Min(workersCount, clusterCandidates.size());
        std::partial_sort(
            clusterCandidates.begin(),
            clusterCandidates.begin() + clusterSelectedCount,
            clusterCandidates.end(),
            compareCandidates);
        ui64 remainingBudget = std::numeric_limits<ui64>::max();
        if (const auto budgetIt = budget.SlotsByCluster.find(clusterName);
            budgetIt != budget.SlotsByCluster.end())
        {
            remainingBudget = budgetIt->second;
        }
        const auto mandatoryIt = remainingMandatorySlots.find(clusterName);
        ui64 mandatorySlots = mandatoryIt != remainingMandatorySlots.end() ? mandatoryIt->second : 0;
        for (size_t i = 0; i < clusterSelectedCount; ++i) {
            auto& candidate = clusterCandidates[i];
            candidate.Slots = Min(candidate.Slots, remainingBudget);
            remainingBudget -= candidate.Slots;
            if (mandatorySlots != 0 && candidate.Slots != 0) {
                if (!candidate.RequiredForMandatoryTasks) {
                    candidate.RequiredForMandatoryTasks = true;
                    ++requiredWorkerCount;
                }
                mandatorySlots -= Min(mandatorySlots, candidate.Slots);
            }
            // Zero-marginal workers may still be needed by task filters.
            marginalCandidates.push_back(std::move(candidate));
        }
        if (mandatorySlots != 0 || requiredWorkerCount > workersCount) {
            return 0;
        }
        remainingMandatorySlots.erase(clusterName);
    }
    if (!remainingMandatorySlots.empty()) {
        return 0;
    }

    const size_t selectedCount = Min(workersCount, marginalCandidates.size());
    std::partial_sort(
        marginalCandidates.begin(),
        marginalCandidates.begin() + selectedCount,
        marginalCandidates.end(),
        compareCandidates);

    ui64 slots = 0;
    for (size_t i = 0; i < selectedCount; ++i) {
        const auto& candidate = marginalCandidates[i];
        slots += candidate.Slots;
        boundedList.insert(candidate.Worker);
        FreeList.erase(candidate.Worker);
    }
    return slots;
}

TWorkersStorage::TFreeList::iterator TWorkersStorage::ProcessMatched(
    TWorkersStorage::TFreeList::iterator it,
    TVector<NDqs::TWorkerInfo::TPtr>& result,
    TFreeList* freeList,
    THashMap<TWorkerInfo::TPtr, int>& tasksPerWorker,
    THashMap<i32, TWorkerFilter>& tasksToAllocate,
    int taskId,
    TAllocationBudget* budget)
{
    auto workerInfo = *it;

    if (budget) {
        const auto taskIt = tasksToAllocate.find(taskId);
        Y_ABORT_UNLESS(taskIt != tasksToAllocate.end());
        budget->Consume(workerInfo, taskIt->second);
    }

    it = freeList->erase(it);
    const auto slotsBeforeAcquire = GetSchedulableSlots(workerInfo);
    if (!workerInfo->Acquire()) {
        freeList->insert(workerInfo);
        it = freeList->begin();
    }
    UpdateSlotsSnapshot(workerInfo, slotsBeforeAcquire);

    tasksPerWorker[workerInfo]++;

    Y_ASSERT(result[taskId] == nullptr);
    result[taskId] = workerInfo;
    tasksToAllocate.erase(taskId);

    return it;
}

size_t TWorkersStorage::SearchFreeList(
    TVector<TWorkerInfo::TPtr>& result,
    TFreeList* freeList,
    const i32 maxTasksPerWorker,
    THashMap<TWorkerInfo::TPtr, int>& tasksPerWorker,
    THashMap<i32, TWorkerFilter>& tasksToAllocate,
    THashMap<TString, THashSet<int>>& waitingResources,
    TWorkerFilter::EMatchStatus matchMode,
    TAllocationBudget* budget,
    size_t maxOuterIters)
{
    TWorkerFilter::TStats stats;
    stats.WaitingResources = &waitingResources;
    stats.Uploaded = &Uploaded;

    size_t outerIters = 0;

    // fresh nodes must be first
    for (auto it = freeList->begin(); it != freeList->end(); ) {
        if (maxOuterIters > 0 && outerIters >= maxOuterIters) {
            break;
        }

        // must exists
        auto workerInfo = *it;
        if (workerInfo->IsDead) {
            YQL_CLOG(DEBUG, ProviderDq) << "Remove dead worker from free list "
                           << GetGuidAsString(workerInfo->WorkerId);
            it = freeList->erase(it);
            continue;
        }
        if (workerInfo->Capacity <= workerInfo->RunningRequests) {
            it = freeList->erase(it);
            continue;
        }

        if (maxTasksPerWorker > 0 && tasksPerWorker[workerInfo] >= maxTasksPerWorker) {
            ++it;
            ++outerIters;
            continue;
        }

        if (workerInfo->Stopping) {
            ++it;
            ++outerIters;
            continue;
        }
        if (budget && GetClusterBudget(workerInfo, &budget->SlotsByCluster) == 0) {
            ++it;
            continue;
        }

        i32 taskId = 0;
        bool ok = false;
        for (const auto& [taskId_, filter] : tasksToAllocate) {
            taskId = taskId_;
            if ((!budget || budget->CanAllocate(workerInfo, filter)) &&
                matchMode == filter.Match(workerInfo, taskId, &stats))
            {
                ok = true;
                break;
            }
        }

        ++outerIters;

        if (ok) {
            it = ProcessMatched(
                it, result, freeList, tasksPerWorker, tasksToAllocate, taskId, budget);
        } else {
            ++it;
        }

        if (tasksToAllocate.empty()) {
            break;
        }
    }

    return outerIters;
}

size_t TWorkersStorage::SearchFreeListWithIndex(
    TVector<TWorkerInfo::TPtr>& result,
    TFreeList* freeList,
    const i32 maxTasksPerWorker,
    THashMap<TWorkerInfo::TPtr, int>& tasksPerWorker,
    THashMap<i32, TWorkerFilter>& tasksToAllocate,
    THashMap<TString, THashSet<int>>& waitingResources,
    TAllocationBudget* budget)
{
    // Snapshot task IDs to avoid iterator invalidation when ProcessMatched
    // erases matched tasks from tasksToAllocate.
    TVector<i32> taskIds;
    taskIds.reserve(tasksToAllocate.size());
    for (const auto& [id, _] : tasksToAllocate) {
        taskIds.push_back(id);
    }

    size_t opsCount = 0;

    for (const auto taskId : taskIds) {
        auto taskIt = tasksToAllocate.find(taskId);
        if (taskIt == tasksToAllocate.end()) {
            continue; // already allocated in a previous iteration
        }
        const auto& filter = taskIt->second;

        // Collect file IDs from this task's filter.
        TVector<TString> fileIds;
        filter.Visit([&](const auto& file) {
            fileIds.push_back(file.GetObjectId());
        });

        if (fileIds.empty()) {
            continue; // no file filter — leave for regular SearchFreeList
        }

        opsCount += fileIds.size(); // index lookups

        // Find the smallest worker set among all required files (best pivot for intersection).
        const THashSet<TWorkerInfo::TPtr>* pivot = nullptr;
        bool anyFileMissing = false;
        for (const auto& fileId : fileIds) {
            auto it = FileToWorkers.find(fileId);
            if (it == FileToWorkers.end() || it->second.empty()) {
                anyFileMissing = true;
                break;
            }
            if (!pivot || it->second.size() < pivot->size()) {
                pivot = &it->second;
            }
        }

        if (anyFileMissing || !pivot) {
            // No worker has all files — task is waiting for missing resources.
            for (const auto& fileId : fileIds) {
                waitingResources[fileId].insert(taskId);
                auto uploadedIt = Uploaded.find(fileId);
                if (uploadedIt != Uploaded.end()) {
                    uploadedIt->second.TryCount++;
                }
            }
            continue; // leave in tasksToAllocate for SearchFreeList fallback
        }

        // Iterate pivot set; check each worker has ALL files and is allocatable.
        bool allocated = false;
        for (const auto& workerInfo : *pivot) {
            opsCount++;

            if (workerInfo->IsDead) {
                continue;
            }
            if (workerInfo->Capacity <= workerInfo->RunningRequests) {
                continue;
            }
            if (workerInfo->Stopping) {
                continue;
            }
            if (maxTasksPerWorker > 0 && tasksPerWorker[workerInfo] >= maxTasksPerWorker) {
                continue;
            }
            if (budget && !budget->CanAllocate(workerInfo, filter)) {
                continue;
            }
            if (!filter.MatchHost(workerInfo)) {
                continue;
            }
            if (freeList->find(workerInfo) == freeList->end()) {
                continue;
            }

            // Verify all required files are present (pivot only guarantees one file).
            bool hasAll = true;
            for (const auto& fileId : fileIds) {
                opsCount++;
                if (!workerInfo->GetResources().contains(fileId)) {
                    hasAll = false;
                    break;
                }
            }
            if (!hasAll) {
                continue;
            }

            // Mark files as available for this task in stats — no longer waiting.
            for (const auto& fileId : fileIds) {
                waitingResources[fileId].erase(taskId);
            }

            // Allocate: find iterator in freeList and call ProcessMatched.
            auto freeIt = freeList->find(workerInfo);
            ProcessMatched(freeIt, result, freeList, tasksPerWorker, tasksToAllocate, taskId, budget);
            allocated = true;
            break;
        }

        if (!allocated) {
            // No suitable worker found via index — leave task for SearchFreeList fallback.
            // (The worker exists in the index but may be busy, stopping, or mismatching host.)
        }
    }

    return opsCount;
}

TVector<TWorkerInfo::TPtr> TWorkersStorage::TryAllocate(
    const NDq::IScheduler::TWaitInfo& waitInfo,
    TLargeSlotsBudgetByCluster* largeSlotsBudgetByCluster) noexcept
{
    bool retryWithBudgeted = false;
    auto result = TryAllocateImpl(
        waitInfo, largeSlotsBudgetByCluster, /*useBudgetedBoundedSelection*/ false, retryWithBudgeted);
    if (retryWithBudgeted) {
        // Preserve legacy placements that fit the budget. Retry with budgeted candidates
        // after the first attempt has rolled back and restored FreeList.
        result = TryAllocateImpl(
            waitInfo, largeSlotsBudgetByCluster, /*useBudgetedBoundedSelection*/ true, retryWithBudgeted);
    }
    return result;
}

TVector<TWorkerInfo::TPtr> TWorkersStorage::TryAllocateImpl(
    const NDq::IScheduler::TWaitInfo& waitInfo,
    TLargeSlotsBudgetByCluster* largeSlotsBudgetByCluster,
    bool useBudgetedBoundedSelection,
    bool& retryWithBudgeted)
{
    retryWithBudgeted = false;
    auto& request = waitInfo.Request;
    auto count = request.GetCount();
    Y_ABORT_UNLESS(count != 0);
    TVector<TWorkerInfo::TPtr> result(count, nullptr);
    auto& filterPerTask = request.GetWorkerFilterPerTask();
    THashMap<i32, TWorkerFilter> tasksToAllocate;

    YQL_LOG_CTX_ROOT_SESSION_SCOPE(waitInfo.Request.GetTraceId());
    YQL_CLOG(DEBUG, ProviderDq) << "TryAllocate, workers count: " << count << " files per task: " << filterPerTask.size();

    if (filterPerTask.empty()) {
        // COMPAT demand all files on all workers
        Yql::DqsProto::TWorkerFilter commonFilter;
        for (const auto& file : request.GetFiles()) {
            *commonFilter.AddFile() = file;
        }
        for (ui32 i = 0; i < count; ++i) {
            tasksToAllocate.emplace(i, commonFilter);
        }
    } else {
        for (i32 i = 0; i < filterPerTask.size(); ++i) {
            tasksToAllocate.emplace(i, filterPerTask.Get(i));
        }
    }

    Y_ASSERT(tasksToAllocate.size() == count);

    TAllocationBudget allocationBudget;
    TAllocationBudget* budget = nullptr;
    if (largeSlotsBudgetByCluster) {
        allocationBudget.SlotsByCluster = *largeSlotsBudgetByCluster;
        for (const auto& [_, filter] : tasksToAllocate) {
            if (const auto& clusterName = filter.GetClusterName(); !clusterName.empty()) {
                ++allocationBudget.MandatorySlotsByCluster[clusterName];
            }
        }
        budget = &allocationBudget;
    }

    auto now = TInstant::Now();

    for (const auto& file : request.GetFiles()) {
        auto id = file.GetObjectId();
        auto maybeUploaded = Uploaded.find(id);
        if (maybeUploaded == Uploaded.end()) {
            Uploaded.emplace(id, TResourceStat{id, file.GetObjectType(), file.GetName(), file.GetSize()});
        } else {
            if (maybeUploaded->second.Name.empty()) {
                maybeUploaded->second = TResourceStat{id, file.GetObjectType(), file.GetName(), file.GetSize()};
            } else {
                maybeUploaded->second.LastSeenTime = now;
            }
        }
    }

    THashMap<TString, THashSet<int>> waitingResources; // resourceId -> taskId

    TFreeList boundedList;
    auto* freeList = &FreeList;

    Y_SCOPE_EXIT(&) {
        for (const auto& workerInfo : boundedList) {
            FreeList.insert(workerInfo);
        }
    };

    i32 maxTasksPerWorker = 0;
    if (request.GetWorkersCount()) {
        YQL_CLOG(DEBUG, ProviderDq) << "Bounded mode, requested workers: " << count
            << ", workers limit: " << request.GetWorkersCount();
        maxTasksPerWorker = (count + request.GetWorkersCount() - 1) / request.GetWorkersCount();
        freeList = &boundedList;
        ui64 slots;
        if (budget) {
            const bool onlyUnconfiguredMandatoryClusters = std::all_of(
                tasksToAllocate.begin(),
                tasksToAllocate.end(),
                [&] (const auto& item) {
                    const auto& clusterName = item.second.GetClusterName();
                    return clusterName && !budget->SlotsByCluster.contains(clusterName);
                });
            if (onlyUnconfiguredMandatoryClusters || !useBudgetedBoundedSelection) {
                retryWithBudgeted = !onlyUnconfiguredMandatoryClusters;
                slots = BuildLegacyBoundedFreeList(
                    boundedList,
                    request.GetWorkersCount(),
                    maxTasksPerWorker);
            } else {
                const auto& mandatoryHostFilter = tasksToAllocate.begin()->second;
                const bool hasCommonMandatoryHostFilter = std::all_of(
                    tasksToAllocate.begin(),
                    tasksToAllocate.end(),
                    [&] (const auto& item) {
                        return mandatoryHostFilter.HasSameMandatoryHostFilter(item.second);
                    });
                slots = BuildBudgetedBoundedFreeList(
                    boundedList,
                    request.GetWorkersCount(),
                    maxTasksPerWorker,
                    *budget,
                    hasCommonMandatoryHostFilter ? &mandatoryHostFilter : nullptr,
                    tasksToAllocate);
            }
        } else {
            slots = BuildLegacyBoundedFreeList(
                boundedList,
                request.GetWorkersCount(),
                maxTasksPerWorker);
        }

        if (slots < count) {
            YQL_CLOG(DEBUG, ProviderDq) << "Not enough slots: available slots=" << slots
                << ", requested workers=" << count
                << ", selected workers=" << freeList->size()
                << ", workers limit=" << request.GetWorkersCount();
            return { };
        }

        if (budget) {
            TLargeSlotsBudgetByCluster availableSlotsByCluster;
            for (const auto& workerInfo : boundedList) {
                if (!workerInfo->IsDead && !workerInfo->Stopping) {
                    availableSlotsByCluster[workerInfo->ClusterName] += Min<ui64>(
                        workerInfo->Capacity - workerInfo->RunningRequests,
                        maxTasksPerWorker);
                }
            }
            for (const auto& [clusterName, availableSlots] : availableSlotsByCluster) {
                auto [budgetIt, inserted] = budget->SlotsByCluster.emplace(clusterName, availableSlots);
                if (!inserted) {
                    budgetIt->second = Min(budgetIt->second, availableSlots);
                }
            }
        }
    }

    // Estimate file lookup work per worker from the remaining tasks.
    // The rounded-down average is a heuristic, not a strict lookup count.
    size_t totalFiles = 0;
    for (const auto& [_, filter] : tasksToAllocate) {
        totalFiles += filter.GetFileCount();
    }
    const size_t avgFilesPerTask = tasksToAllocate.empty() ? 1
        : Max<size_t>(1, totalFiles / tasksToAllocate.size());

    // Each linear pass (EOK and EPARTIAL) gets its own cap, with at least one
    // iteration when limiting is enabled. 0 = unlimited.
    const size_t maxOuterItersPerPass = (SearchComplexityLimit > 0 && !tasksToAllocate.empty())
        ? Max<size_t>(1, SearchComplexityLimit / (tasksToAllocate.size() * avgFilesPerTask))
        : 0;

    THashMap<TWorkerInfo::TPtr, int> tasksPerWorker;
    size_t totalIters = 0;

    // Fast path: use inverted index for tasks with file filters.
    // Handles tasks where all required files are available on some workers
    // without scanning the full FreeList.
    if (!FileToWorkers.empty()) {
        totalIters += SearchFreeListWithIndex(result, freeList, maxTasksPerWorker,
                                              tasksPerWorker, tasksToAllocate, waitingResources, budget);
    }

    // Fallback: linear scan for remaining tasks (no file filter, or index had no candidates).
    if (!tasksToAllocate.empty()) {
        totalIters += SearchFreeList(result, freeList, maxTasksPerWorker, tasksPerWorker, tasksToAllocate, waitingResources, TWorkerFilter::EOK, budget, maxOuterItersPerPass);
    }
    if (!tasksToAllocate.empty()) {
        totalIters += SearchFreeList(result, freeList, maxTasksPerWorker, tasksPerWorker, tasksToAllocate, waitingResources, TWorkerFilter::EPARTIAL, budget, maxOuterItersPerPass);
    }

    if (SearchIterationsCounter) {
        *SearchIterationsCounter += totalIters;
    }
    if (TryAllocateOverloadedCounter && maxOuterItersPerPass > 0 && totalIters >= maxOuterItersPerPass && !tasksToAllocate.empty()) {
        *TryAllocateOverloadedCounter += 1;
        YQL_CLOG(WARN, ProviderDq) << "TryAllocate search truncated: iters=" << totalIters
            << " limit=" << maxOuterItersPerPass << " remainingTasks=" << tasksToAllocate.size();
    }

    if (!tasksToAllocate.empty()) {
        for (const auto& workerInfo : result) {
            if (workerInfo == nullptr) {
                continue;
            }
            freeList->erase(workerInfo);
            const auto slotsBeforeRelease = GetSchedulableSlots(workerInfo);
            if (workerInfo->Release()) {
                freeList->insert(workerInfo);
            }
            UpdateSlotsSnapshot(workerInfo, slotsBeforeRelease);
        }
        if (retryWithBudgeted) {
            return {};
        }
    }

    for (const auto& file : request.GetFiles()) {
        const auto& id = file.GetObjectId();

        if (waitingResources.contains(id)) {
            waitInfo.Stat.StartCounter(id, now);
        } else {
            waitInfo.Stat.FlushCounter(id);
        }
    }

    waitInfo.ResLeft.clear();
    if (tasksToAllocate.empty()) {
        retryWithBudgeted = false;
        auto now = TInstant::Now();
        for (const auto& workerInfo : result) {
            Y_ASSERT(workerInfo != nullptr);
            ConsumeClusterBudget(workerInfo, 1, largeSlotsBudgetByCluster);
            workerInfo->UseCount++;
            workerInfo->RequestStartTime = now;
        }
        for (const auto& file : request.GetFiles()) {
            auto id = file.GetObjectId();
            auto it = Uploaded.find(id);
            auto delta = waitInfo.Stat.Get().contains(id)
                ? TDuration::MilliSeconds(waitInfo.Stat.Get().find(id)->second.Sum)
                : TDuration();

            waitInfo.Stat.Clear();

            it->second.UseCount += count;
            it->second.WaitTime += delta;

            TString resourceName;

            switch (file.GetObjectType()) {
            case Yql::DqsProto::TFile::EEXE_FILE:
                resourceName = "ResourceExe";
                waitInfo.Stat.AddCounter(resourceName, delta);
                break;
            case Yql::DqsProto::TFile::EUDF_FILE:
                if (!file.GetName().empty()) {
                    resourceName = "ResourceUdf" + file.GetName();
                    waitInfo.Stat.AddCounter(resourceName, delta);
                }
                waitInfo.Stat.AddCounter("ResourceUdf", delta);
                break;
            default:
                resourceName = "ResourceFile";
                waitInfo.Stat.AddCounter(resourceName, delta);
                break;
            }

            ResourceWaitTime->Collect(delta.MilliSeconds());
            *ResourceCounters->GetCounter(resourceName, /*derivative=*/ true) += count;
        }

        return result;
    } else {
        YQL_CLOG(DEBUG, ProviderDq) << "Tasks left " << tasksToAllocate.size();
        // schedule downloads
        for (const auto& [_, filter] : tasksToAllocate) {
            filter.Visit([&](const auto& file) {
                waitInfo.ResLeft[file.GetObjectId()]++;
            });
        }

        const auto firstDownloadTask = std::find_if(
            tasksToAllocate.begin(), tasksToAllocate.end(), [] (const auto& task) {
                return task.second.GetFileCount() != 0;
            });
        if (firstDownloadTask == tasksToAllocate.end()) {
            return {};
        }
        const auto& firstFilter = firstDownloadTask->second;
        const bool commonHostFilter = std::all_of(
            tasksToAllocate.begin(), tasksToAllocate.end(), [&] (const auto& task) {
                return task.second.GetFileCount() == 0 || firstFilter.HasSameMandatoryHostFilter(task.second);
            });
        // Prefer receivers that can extend the partial allocation. After rollback,
        // however, that placement is only a hint: downloading to a released slot
        // may enable a different placement on the next attempt.
        const auto fitsPartialAllocation = [&] (const auto& worker, const auto& filter) {
            const auto* allocatedTasks = tasksPerWorker.FindPtr(worker);
            const int used = allocatedTasks ? *allocatedTasks : 0;
            return worker->Capacity - worker->RunningRequests > used &&
                (maxTasksPerWorker == 0 || used < maxTasksPerWorker) &&
                (!budget || budget->CanAllocate(worker, filter));
        };
        TVector<TWorkerInfo::TPtr> downloadCandidates;
        TVector<TWorkerInfo::TPtr> preferredCandidates;
        for (const auto& worker : *freeList) {
            if (!worker->IsDead && !worker->Stopping && worker->Capacity > worker->RunningRequests &&
                GetClusterBudget(worker, largeSlotsBudgetByCluster) != 0 &&
                (!commonHostFilter || firstFilter.MatchHost(worker)))
            {
                downloadCandidates.push_back(worker);
                if (commonHostFilter && fitsPartialAllocation(worker, firstFilter)) {
                    preferredCandidates.push_back(worker);
                }
            }
        }
        if (commonHostFilter && !preferredCandidates.empty()) {
            downloadCandidates = std::move(preferredCandidates);
        }
        size_t nextCandidate = 0;
        for (const auto& [_, filter] : tasksToAllocate) {
            if (filter.GetFileCount() == 0) {
                continue;
            }
            TWorkerInfo::TPtr workerInfo;
            for (bool preferPartial : {true, false}) {
                for (size_t checked = 0; checked < downloadCandidates.size(); ++checked) {
                    const auto& candidate = downloadCandidates[nextCandidate];
                    nextCandidate = (nextCandidate + 1) % downloadCandidates.size();
                    if (commonHostFilter || (filter.MatchHost(candidate) &&
                        (!preferPartial || fitsPartialAllocation(candidate, filter))))
                    {
                        workerInfo = candidate;
                        break;
                    }
                }
                if (workerInfo || commonHostFilter) {
                    break;
                }
            }
            if (!workerInfo) {
                continue;
            }
            filter.Visit([&](const auto& file) {
                TWorkerInfo::TDownloadResource resource{
                    .File = file,
                    .UploadPrefix = request.GetUploadPrefix(),
                    .UploadClusterAddress = request.GetUploadClusterAddress(),
                };
                if (workerInfo->AddToDownloadList(resource)) {
                    YQL_CLOG(TRACE, ProviderDq) << "Added " << file.GetName() << "|" << file.GetObjectId() << " to worker's " << GetGuidAsString(workerInfo->WorkerId) << " download list" ;
                    TStringBuilder resourceDownloadName;
                    resourceDownloadName << "ResourceDownload";
                    switch (file.GetObjectType()) {
                        case Yql::DqsProto::TFile::EEXE_FILE:
                            resourceDownloadName << "Exe";
                            break;
                        case Yql::DqsProto::TFile::EUDF_FILE:
                            resourceDownloadName << "Udf" << file.GetName();
                            break;
                        default:
                            resourceDownloadName << "File";
                            break;
                    }
                    *ResourceDownloadCounters->GetCounter(resourceDownloadName, /*derivative=*/ true) += 1;
                }
            });
        }

        return { };
    }
}

void TWorkersStorage::IsReady(const TVector<TFileResource>& resources, THashMap<TString, std::pair<ui32, ui32>>& clusterMap) {
    for (const auto& [_,workerInfo] : Workers) {
        auto count = 0;
        for (const auto& r : resources){
            if (workerInfo->GetResources().contains(r.GetObjectId())) {
                count++;
            }
        }
        YQL_CLOG(TRACE, ProviderDq) << workerInfo->ClusterName << " worker " << GetGuidAsString(workerInfo->WorkerId) << " has: " << count << "/" << int(resources.size()) << " resources";
        clusterMap[workerInfo->ClusterName].second += count == int(resources.size());
    }
};

void TWorkersStorage::ClusterStatus(Yql::DqsProto::ClusterStatusResponse* r) const {
    for (const auto& [_, workerInfo] : Workers) {
        auto* node = r->AddWorker();
        node->SetGuid(GetGuidAsString(workerInfo->WorkerId));
        node->SetNodeId(workerInfo->NodeId);
        node->SetLastPingTime(workerInfo->LastPingTime.ToString());
        node->SetRevision(workerInfo->Revision);
        node->SetClusterName(workerInfo->ClusterName);
        node->SetAddress(workerInfo->Address);
        node->SetPort(workerInfo->Port);
        node->SetEpoch(workerInfo->Epoch);
        node->SetStopping(workerInfo->Stopping);
        node->SetStartTime(workerInfo->StartTime);

        for (const auto& f : workerInfo->GetResourcesOrdered()) {
            *node->AddResource() = f.GetObjectId();
        }

        for (const auto& [k, _] : workerInfo->GetDownloadList()) {
            node->AddDownloadList()->SetObjectId(k.ObjectId);
        }

        for (const auto& [k, _] : workerInfo->GetActiveDownloads()) {
            node->AddActiveDownload()->SetObjectId(k.ObjectId);
        }
        for (const auto& op : workerInfo->Operations){
            node->AddOperation(op);
        }
        node->SetFreeDiskSize(workerInfo->FreeDiskSize);
        node->SetUsedDiskSize(workerInfo->UsedDiskSize);
        node->SetCapacity(workerInfo->Capacity);
        node->SetRunningWorkerActors(workerInfo->RunningWorkerActors);
        node->SetRunningRequests(workerInfo->RunningRequests);
        node->SetDead(workerInfo->IsDead);

        node->MutableRusage()->SetStime(workerInfo->Stime.MicroSeconds());
        node->MutableRusage()->SetUtime(workerInfo->Utime.MicroSeconds());
        node->MutableRusage()->SetMaxRss(workerInfo->MaxRss);
        node->MutableRusage()->SetCurrentRss(workerInfo->CurrentRss);
        node->MutableRusage()->SetMajorPageFaults(workerInfo->MajorPageFaults);
        node->MutableRusage()->SetCpuSystem(workerInfo->CpuSystem);
        node->MutableRusage()->SetCpuUser(workerInfo->CpuUser);
        node->MutableRusage()->SetCpuTotal(workerInfo->CpuTotal);

        for (const auto& [k, v] : workerInfo->Attributes) {
            auto* attr = node->AddAttribute();
            attr->SetKey(k);
            attr->SetValue(v);
        }

        node->SetUseCount(workerInfo->UseCount);
        node->SetUseTime(workerInfo->UseTime.MilliSeconds());
        if (workerInfo->RunningRequests > 0) {
            node->SetRequestStartTime(workerInfo->RequestStartTime.ToString());
        }
    }

    for (const auto& [_, i] : Uploaded) {
        auto* rr = r->AddResource();
        rr->SetId(i.Id);
        rr->SetName(i.Name);
        rr->SetUseTime(i.UseTime.MilliSeconds());
        rr->SetWaitTime(i.WaitTime.MilliSeconds());
        rr->SetUseCount(i.UseCount);
        rr->SetTryCount(i.TryCount);
        rr->SetSize(i.Size);
        rr->SetUploaded(i.Uploaded);
    }

    r->SetFreeListSize(FreeList.size());
    r->SetCapacity(GlobalResources.GetCapacity());
    r->SetRunningRequests(GlobalResources.GetRunningRequests());
}

void TWorkersStorage::UpdateResourceUseTime(TDuration duration, const THashSet<TString>& ids) {
    for (const auto& id : ids) {
        auto it = Uploaded.find(id);
        if (it != Uploaded.end()) {
            it->second.UseTime += duration;
        }
    }
}

bool TWorkersStorage::HasResource(const TString& id) const {
    auto it = Uploaded.find(id);
    return (it != Uploaded.end()) && it->second.Uploaded;
}

void TWorkersStorage::AddResource(const TString& id, EFileType type, const TString& name, i64 size) {
    if (!Uploaded.contains(id)) {
        Uploaded.emplace(id, TResourceStat{id, type, name, size});
    }
    Uploaded.find(id)->second.Uploaded = true;
}

void TWorkersStorage::UpdateMetrics() {
    if (!WorkersSize) {
        WorkersSize = Metrics->GetSubgroup("component", "lists")->GetCounter("WorkersSize");
    }
    if (!FreeListSize) {
        FreeListSize = Metrics->GetSubgroup("component", "lists")->GetCounter("FreeListSize");
    }
    if (!SearchIterationsCounter) {
        SearchIterationsCounter = Metrics->GetSubgroup("component", "search")->GetCounter("SearchIterations", /*derivative=*/true);
    }
    if (!TryAllocateOverloadedCounter) {
        TryAllocateOverloadedCounter = Metrics->GetSubgroup("component", "search")->GetCounter("TryAllocateOverloaded", /*derivative=*/true);
    }
    *WorkersSize = Workers.size();
    *FreeListSize = FreeList.size();
}

void TWorkersStorage::Visit(const std::function<void(const TWorkerInfo::TPtr& workerInfo)>& f) {
    for (const auto& [_, workerInfo] : Workers) {
        f(workerInfo);
    }
}

} // namespace NYql
