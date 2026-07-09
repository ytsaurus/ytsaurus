#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

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

    // Required: the caller must provide the ITimers implementation.
    // Use NTables::TTimers for production and TInMemoryTimers for tests.
    NTables::ITimersPtr TimersTable;
};

DEFINE_REFCOUNTED_TYPE(TTimerStoreContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTimerStoreContext
    : public TRefCounted
{
    TDynamicTimerStoreSpecPtr DynamicTimerStoreSpec;
    bool Draining{};
};

DEFINE_REFCOUNTED_TYPE(TDynamicTimerStoreContext);

////////////////////////////////////////////////////////////////////////////////

struct ITimerStore
    : public TRefCounted
{
    virtual std::vector<TInputTimerConstPtr> GetNextBatch(const THashSet<TStreamId>& allowedStreams, i64 maxRows, i64 maxByteSize) = 0;

    virtual void Reconfigure(TDynamicTimerStoreContextPtr dynamicContext) = 0;

    virtual void UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;

    virtual void Register(std::vector<TTimer>&& timers) = 0;
    virtual void Unregister(const std::vector<TInputTimerConstPtr>& timers) = 0;

    virtual TFuture<void> Init() = 0;
    virtual void Sync(NApi::IDynamicTableTransactionPtr tx) = 0;

    virtual THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() = 0;

    virtual i64 GetByteSize() const = 0;
    virtual i64 GetCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimerStore);

////////////////////////////////////////////////////////////////////////////////

ITimerStorePtr CreateTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
