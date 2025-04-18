#pragma once

#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/table_service_config.pb.h>
#include <contrib/ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <contrib/ydb/core/kqp/counters/kqp_counters.h>
#include <yql/essentials/minikql/computation/mkql_computation_pattern_cache.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/stream/format.h>

#include "kqp_resource_estimation.h"

#include <array>
#include <bitset>
#include <functional>
#include <utility>


namespace NKikimr {
namespace NKqp {

namespace NRm {

/// memory pools
enum EKqpMemoryPool : ui32 {
    Unspecified = 0,
    ScanQuery   = 1, // slow allocations via ResourceBroker
    DataQuery   = 2, // fast allocations via memory-arena

    Count = 3
};

using TOnResourcesSnapshotCallback = std::function<void(TVector<NKikimrKqp::TKqpNodeResources>&&)>;

/// resources request
struct TKqpResourcesRequest {
    ui64 ExecutionUnits = 0;
    EKqpMemoryPool MemoryPool = EKqpMemoryPool::Unspecified;
    ui64 Memory = 0;
    ui64 ExternalMemory = 0;
    bool ReleaseAllResources = false;

    void MoveToFreeTier() {
        ExternalMemory += Memory;
        Memory = 0;
    }

    TString ToString() const {
        return TStringBuilder() << "TKqpResourcesRequest{ MemoryPool: " << (ui32) MemoryPool << ", Memory: " << Memory
            << "ExternalMemory: " << ExternalMemory << " }";
    }
};

class TTxState;

class TMemoryResourceCookie : public TAtomicRefCount<TMemoryResourceCookie> {
public:
    std::atomic<bool> SpillingPercentReached{false};
};

class TTaskState : public TAtomicRefCount<TTaskState> {
    friend TTxState;

public:
    const ui64 TaskId = 0;
    const TInstant CreatedAt;
    ui64 ScanQueryMemory = 0;
    ui64 ExternalDataQueryMemory = 0;
    ui64 ResourceBrokerTaskId = 0;
    ui32 ExecutionUnits = 0;
    TIntrusivePtr<TMemoryResourceCookie> TotalMemoryCookie;
    TIntrusivePtr<TMemoryResourceCookie> PoolMemoryCookie;

public:

    // compute actor wants to release some memory.
    // we distribute that memory across granted resources
    TKqpResourcesRequest FitRequest(TKqpResourcesRequest& resources) {
        ui64 releaseScanQueryMemory = std::min(ScanQueryMemory, resources.Memory);
        ui64 leftToRelease = resources.Memory - releaseScanQueryMemory;
        ui64 releaseExternalDataQueryMemory = std::min(ExternalDataQueryMemory, resources.ExternalMemory + leftToRelease);

        resources.Memory = releaseScanQueryMemory;
        resources.ExternalMemory = releaseExternalDataQueryMemory;
        return resources;
    }

    bool IsReasonableToStartSpilling() {
        return (PoolMemoryCookie && PoolMemoryCookie->SpillingPercentReached.load())
            || (TotalMemoryCookie && TotalMemoryCookie->SpillingPercentReached.load());
    }

    TKqpResourcesRequest FreeResourcesRequest() const {
        return TKqpResourcesRequest{
            .ExecutionUnits=ExecutionUnits,
            .MemoryPool=EKqpMemoryPool::Unspecified,
            .Memory=ScanQueryMemory,
            .ExternalMemory=ExternalDataQueryMemory};
    }

    explicit TTaskState(ui64 taskId, TInstant createdAt)
        : TaskId(taskId)
        , CreatedAt(createdAt)
    {
    }
};

class TTxState : public TAtomicRefCount<TTxState> {

public:
    const ui64 TxId;
    const TInstant CreatedAt;
    TIntrusivePtr<TKqpCounters> Counters;
    const TString PoolId;
    const double MemoryPoolPercent;
    const TString Database;
    const bool CollectBacktrace;

private:
    std::atomic<ui64> TxScanQueryMemory = 0;
    std::atomic<ui64> TxExternalDataQueryMemory = 0;
    std::atomic<ui32> TxExecutionUnits = 0;
    std::atomic<ui64> TxMaxAllocationSize = 0;

    // TODO(ilezhankin): it's better to use std::atomic<std::shared_ptr<>> which is not supported at the moment.
    std::atomic<TBackTrace*> TxMaxAllocationBacktrace = nullptr;

    // NOTE: it's hard to maintain atomic pointer in case of tracking the last failed allocation backtrace,
    //       because while we try to print one - the new last may emerge and delete previous.
    mutable std::mutex BacktraceMutex;
    std::atomic<ui64> TxFailedAllocationSize = 0; // protected by BacktraceMutex (only if CollectBacktrace == true)
    TBackTrace TxFailedAllocationBacktrace;       // protected by BacktraceMutex
    std::atomic<bool> HasFailedAllocationBacktrace = false;

public:
    explicit TTxState(ui64 txId, TInstant now, TIntrusivePtr<TKqpCounters> counters, const TString& poolId, const double memoryPoolPercent,
        const TString& database, bool collectBacktrace)
        : TxId(txId)
        , CreatedAt(now)
        , Counters(std::move(counters))
        , PoolId(poolId)
        , MemoryPoolPercent(memoryPoolPercent)
        , Database(database)
        , CollectBacktrace(collectBacktrace)
    {}

    ~TTxState() {
        delete TxMaxAllocationBacktrace.load();
    }

    std::pair<TString, TString> MakePoolId() const {
        return std::make_pair(Database, PoolId);
    }

    TString ToString() const {
        // use unique_lock to safely unlock mutex in case of exceptions
        std::unique_lock backtraceLock(BacktraceMutex, std::defer_lock);

        auto res = TStringBuilder() << "TxResourcesInfo { "
            << "TxId: " << TxId
            << ", Database: " << Database;

        if (!PoolId.empty()) {
            res << ", PoolId: " << PoolId
                << ", MemoryPoolPercent: " << Sprintf("%.2f", MemoryPoolPercent > 0 ? MemoryPoolPercent : 100);
        }

        if (CollectBacktrace) {
            backtraceLock.lock();
        }

        res << ", tx initially granted memory: " << HumanReadableSize(TxExternalDataQueryMemory.load(), SF_BYTES)
            << ", tx total memory allocations: " << HumanReadableSize(TxScanQueryMemory.load(), SF_BYTES)
            << ", tx largest successful memory allocation: " << HumanReadableSize(TxMaxAllocationSize.load(), SF_BYTES)
            << ", tx last failed memory allocation: " << HumanReadableSize(TxFailedAllocationSize.load(), SF_BYTES)
            << ", tx total execution units: " << TxExecutionUnits.load()
            << ", started at: " << CreatedAt
            << " }" << Endl;

        if (CollectBacktrace && HasFailedAllocationBacktrace.load()) {
            res << "TxFailedAllocationBacktrace:" << Endl << TxFailedAllocationBacktrace.PrintToString();
        }

        if (CollectBacktrace) {
            backtraceLock.unlock();
        }

        if (CollectBacktrace && TxMaxAllocationBacktrace.load()) {
            res << "TxMaxAllocationBacktrace:" << Endl << TxMaxAllocationBacktrace.load()->PrintToString();
        }

        return res;
    }

    ui64 GetExtraMemoryAllocatedSize() {
        return TxScanQueryMemory.load();
    }

    void AckFailedMemoryAlloc(ui64 memory) {
        // use unique_lock to safely unlock mutex in case of exceptions
        std::unique_lock backtraceLock(BacktraceMutex, std::defer_lock);

        if (CollectBacktrace) {
            backtraceLock.lock();
        }

        TxFailedAllocationSize = memory;

        if (CollectBacktrace) {
            TxFailedAllocationBacktrace.Capture();
            HasFailedAllocationBacktrace = true;
            backtraceLock.unlock();
        }
    }

    void Released(TIntrusivePtr<TTaskState>& taskState, const TKqpResourcesRequest& resources) {
        if (resources.ExecutionUnits) {
            Counters->RmOnCompleteFree->Inc();
        } else {
            Counters->RmExtraMemFree->Inc();
        }

        Counters->RmExternalMemory->Sub(resources.ExternalMemory);
        TxExternalDataQueryMemory.fetch_sub(resources.ExternalMemory);
        taskState->ExternalDataQueryMemory -= resources.ExternalMemory;

        TxScanQueryMemory.fetch_sub(resources.Memory);
        taskState->ScanQueryMemory -= resources.Memory;
        Counters->RmMemory->Sub(resources.Memory);

        TxExecutionUnits.fetch_sub(resources.ExecutionUnits);
        taskState->ExecutionUnits -= resources.ExecutionUnits;
        Counters->RmComputeActors->Sub(resources.ExecutionUnits);
    }

    void Allocated(TIntrusivePtr<TTaskState>& taskState, const TKqpResourcesRequest& resources) {
        if (resources.ExecutionUnits > 0) {
            Counters->RmOnStartAllocs->Inc();
        }

        Counters->RmExternalMemory->Add(resources.ExternalMemory);
        TxExternalDataQueryMemory.fetch_add(resources.ExternalMemory);
        taskState->ExternalDataQueryMemory += resources.ExternalMemory;

        TxScanQueryMemory.fetch_add(resources.Memory);
        taskState->ScanQueryMemory += resources.Memory;
        Counters->RmMemory->Add(resources.Memory);
        if (resources.Memory) {
            Counters->RmExtraMemAllocs->Inc();
        }

        auto* oldBacktrace = TxMaxAllocationBacktrace.load();
        ui64 maxAlloc = TxMaxAllocationSize.load();
        bool exchanged = false;

        while(maxAlloc < resources.Memory && !exchanged) {
            exchanged = TxMaxAllocationSize.compare_exchange_weak(maxAlloc, resources.Memory);
        }

        if (exchanged) {
            auto* newBacktrace = new TBackTrace();
            newBacktrace->Capture();
            if (TxMaxAllocationBacktrace.compare_exchange_strong(oldBacktrace, newBacktrace)) {
                // XXX(ilezhankin): technically it's possible to have a race with `ToString()`, but it's very unlikely.
                delete oldBacktrace;
            } else {
                delete newBacktrace;
            }
        }

        TxExecutionUnits.fetch_add(resources.ExecutionUnits);
        taskState->ExecutionUnits += resources.ExecutionUnits;
        Counters->RmComputeActors->Add(resources.ExecutionUnits);
    }
};

/// detailed information on allocation failure
struct TKqpRMAllocateResult {
    bool Success = true;
    NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason Status = NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR;
    TString FailReason;
    TIntrusivePtr<TTaskState> TaskInfo;
    TIntrusivePtr<TTxState> TxInfo;

    NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason GetStatus() const {
        return Status;
    }

    TString GetFailReason() const {
        return FailReason;
    }

    void SetError(NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason status, const TString& reason) {
        Success = false;
        Status = status;
        FailReason = reason;
    }

    operator bool() const noexcept {
        return Success;
    }
};

/// local resources snapshot
struct TKqpLocalNodeResources {
    ui32 ExecutionUnits = 0;
    std::array<ui64, EKqpMemoryPool::Count> Memory;
};

struct TPlannerPlacingOptions {
    ui64 MaxNonParallelTasksExecutionLimit = 8;
    ui64 MaxNonParallelDataQueryTasksLimit = 1000;
    ui64 MaxNonParallelTopStageExecutionLimit = 1;
    bool PreferLocalDatacenterExecution = true;
};

/// per node singleton with instant API
class IKqpResourceManager : private TNonCopyable {
public:
    virtual ~IKqpResourceManager() = default;

    virtual const TIntrusivePtr<TKqpCounters>& GetCounters() const = 0;

    virtual TKqpRMAllocateResult AllocateResources(TIntrusivePtr<TTxState>& tx, TIntrusivePtr<TTaskState>& task, const TKqpResourcesRequest& resources) = 0;

    virtual TPlannerPlacingOptions GetPlacingOptions() = 0;
    virtual TTaskResourceEstimation EstimateTaskResources(const NYql::NDqProto::TDqTask& task, const ui32 tasksCount) = 0;
    virtual void EstimateTaskResources(TTaskResourceEstimation& result, const ui32 tasksCount) = 0;

    virtual void FreeResources(TIntrusivePtr<TTxState>& tx, TIntrusivePtr<TTaskState>& task, const TKqpResourcesRequest& resources) = 0;
    virtual void FreeResources(TIntrusivePtr<TTxState>& tx, TIntrusivePtr<TTaskState>& task) = 0;
    virtual void RequestClusterResourcesInfo(TOnResourcesSnapshotCallback&& callback) = 0;

    virtual TVector<NKikimrKqp::TKqpNodeResources> GetClusterResources() const = 0;
    virtual TKqpLocalNodeResources GetLocalResources() const = 0;

    virtual std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> GetPatternCache() = 0;

    virtual ui32 GetNodeId() {
        return 0;
    }
};


struct TResourceSnapshotState {
    std::shared_ptr<TVector<NKikimrKqp::TKqpNodeResources>> Snapshot;
    TMutex Lock;
};

struct TEvKqpResourceInfoExchanger {
    struct TEvPublishResource : public TEventLocal<TEvPublishResource,
        TKqpResourceInfoExchangerEvents::EvPublishResource>
    {
        const NKikimrKqp::TKqpNodeResources Resources;
        TEvPublishResource(NKikimrKqp::TKqpNodeResources resources) : Resources(std::move(resources)) {
        }
    };

    struct TEvSendResources : public TEventPB<TEvSendResources, NKikimrKqp::TResourceExchangeSnapshot,
        TKqpResourceInfoExchangerEvents::EvSendResources>
    {};
};

NActors::IActor* CreateKqpResourceInfoExchangerActor(TIntrusivePtr<TKqpCounters> counters,
    std::shared_ptr<TResourceSnapshotState> resourceSnapshotState,
    const NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings& settings);

} // namespace NRm

struct TKqpProxySharedResources {
    std::atomic<ui32> AtomicLocalSessionCount{0};
};

NActors::IActor* CreateKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
    TIntrusivePtr<TKqpCounters> counters, NActors::TActorId resourceBroker = {},
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources = nullptr,
    ui32 nodeId = 0);

std::shared_ptr<NRm::IKqpResourceManager> GetKqpResourceManager(TMaybe<ui32> nodeId = Nothing());
std::shared_ptr<NRm::IKqpResourceManager> TryGetKqpResourceManager(TMaybe<ui32> nodeId = Nothing());

} // namespace NKqp
} // namespace NKikimr
