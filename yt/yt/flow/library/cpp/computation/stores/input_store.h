#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TInputStoreContext
    : public TRefCounted
{
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

    TPartitionPtr Partition;
    THashSet<TStreamId> InputStreamIds;

    NTables::IInputMessagesPtr InputMessagesTable;
};

DEFINE_REFCOUNTED_TYPE(TInputStoreContext);

////////////////////////////////////////////////////////////////////////////////

struct IInputStore
    : public TRefCounted
{
    struct TFilterResult
    {
        std::vector<TInputMessageConstPtr> Processed;
        std::vector<TInputMessageConstPtr> Unprocessed;
    };

    virtual void Reconfigure(TDynamicInputStoreSpecPtr dynamicSpec) = 0;

    virtual void AdvanceSystemWatermark(TSystemTimestamp systemWatermark) = 0;
    virtual TSystemTimestamp GetSystemWatermark() const = 0;

    virtual TFilterResult Filter(const std::vector<TInputMessageConstPtr>& messages, bool checkState) = 0;

    virtual void Register(const std::vector<TInputMessageConstPtr>& messages) = 0;

    virtual TFuture<void> Init() = 0;
    virtual void Sync(NApi::IDynamicTableTransactionPtr tx) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInputStore);

////////////////////////////////////////////////////////////////////////////////

IInputStorePtr CreateInputStore(TInputStoreContextPtr context, TDynamicInputStoreSpecPtr dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
