#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/trigger_condition.h>

#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TTimerStoreContext
    : public TRefCounted
{
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

    TPartitionPtr Partition;
    NTableClient::TTableSchemaPtr KeySchema;
    THashMap<TStreamId, THashSet<TStreamId>> StreamsDependency;
    THashMap<TStreamId, TTimerSpecPtr> TimerSpecs;
    TWatermarkPercentileSpecPtr WatermarkPercentileSpec;

    ETimerStoreBackend Backend = ETimerStoreBackend::InMemory;

    // Required: the caller must provide the ITimers implementation.
    // Use NTables::TTimers for production and TInMemoryTimers for tests.
    NTables::ITimersPtr TimersTable;

    // Paged backend; the in-memory backend uses them to export pages left by a backend switch.
    i64 TimerPageSize = 0;
    NTables::ITimerPagesPtr TimerPagesTable;
    TTimerPageCataloguePtr PageCatalogue;
    ITimeProviderPtr TimeProvider;
    //! How long |Init| waits for the owner of a shared page to convert its row form.
    TDuration RowFormWaitTimeout = TDuration::Minutes(10);
    //! Commits the conversions of |Init| under the job's lease; null in unit tests.
    NTables::TTransactionManagerPtr TransactionManager;
};

DEFINE_REFCOUNTED_TYPE(TTimerStoreContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTimerStoreContext
    : public TRefCounted
{
    TDynamicTimerStoreSpecPtr DynamicTimerStoreSpec;
    bool Draining{};
    //! The batch size the computation asks for; sizes the resident window of the paged backend.
    i64 MaxRowsPerBatch = 1000;
};

DEFINE_REFCOUNTED_TYPE(TDynamicTimerStoreContext);

////////////////////////////////////////////////////////////////////////////////

struct ITimerStore
    : public TRefCounted
{
    virtual std::vector<TInputTimerConstPtr> GetNextBatch(const THashSet<TStreamId>& allowedStreams, i64 maxRows, i64 maxByteSize) = 0;

    virtual void Reconfigure(TDynamicTimerStoreContextPtr dynamicContext) = 0;

    virtual void UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;

    //! Reads the pages the iteration may fire and does the maintenance work; called before
    //! the watermark update of every iteration.
    virtual TFuture<void> Preload();

    virtual void Register(std::vector<TTimer>&& timers) = 0;
    virtual void Unregister(const std::vector<TInputTimerConstPtr>& timers) = 0;

    virtual TFuture<void> Init() = 0;
    virtual void Sync(NApi::IDynamicTableTransactionPtr tx) = 0;
    virtual void Commit() = 0;

    virtual THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() = 0;

    // The methods below have defaults instead of being pure virtual: implementations outside
    // Flow predate them and override only #GetCount() and #GetByteSize().

    //! Whether the store holds no timers at all, persisted ones included.
    virtual bool IsEmpty() const;
    //! Number of timers the store holds, persisted ones included; an upper bound when the
    //! exact number is unknown.
    virtual i64 GetCount() const = 0;

    //! Memory accounting of the timers currently held in memory; used for the memory limits.
    //! Exactly one of #GetInMemoryByteSize() and #GetByteSize() must be overridden.
    virtual i64 GetInMemoryByteSize() const;
    virtual i64 GetInMemoryCount() const;
    //! Deprecated; use #GetInMemoryByteSize().
    virtual i64 GetByteSize() const;
};

DEFINE_REFCOUNTED_TYPE(ITimerStore);

////////////////////////////////////////////////////////////////////////////////

std::vector<std::vector<TStreamId>> BuildTimerStreamGroups(const TTimerTriggerConditions& conditions);

ITimerStorePtr CreateTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
