#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/stream_inflight_limits.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TOutputStoreContext
    : public TRefCounted
{
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

    TPartitionPtr Partition;
    THashSet<TStreamId> OutputStreamIds;
    TWatermarkPercentileSpecPtr WatermarkPercentileSpec;
    TComputationStreamSpecStoragePtr StreamSpecStorage;

    TStreamLimitUsageStateMap StreamLimitUsageStates;
};

DEFINE_REFCOUNTED_TYPE(TOutputStoreContext);

////////////////////////////////////////////////////////////////////////////////

struct IOutputStore
    : public TRefCounted
{
    virtual void Reconfigure(TDynamicOutputStoreSpecPtr dynamicSpec) = 0;

    virtual bool Contains(const TMessageMeta& message) const = 0;

    virtual void RegisterBatch(std::span<const TOutputMessageConstPtr> messages, bool persist = true) = 0;
    virtual void TryRegisterBatch(std::span<const TOutputMessageConstPtr> messages, bool persist = true) = 0;
    virtual void TryRegisterKeyedBatch(std::span<const TOutputMessageConstPtr> messages, const TKey& key, bool persist = true) = 0;

    virtual void TryUnregisterBatch(std::span<const TMessageMeta* const> metas) = 0;

    //! Convenience overload: accepts any contiguous range of smart/raw pointers to TMessageMeta-derived objects.
    template <class TRange>
    void TryUnregisterBatch(const TRange& messages);

    virtual TFuture<std::vector<std::pair<TOutputMessageConstPtr, std::optional<TKey>>>> Init(bool loadKeyState) = 0;

    //! Enriches |epochTransaction| with all pending output writes plus up to the epoch-tx erase
    //! budget of GC erases. Every remaining (overflow) GC erase is packed into freshly created,
    //! independent transactions and returned; the caller MUST commit ALL of them AFTER the epoch
    //! transaction (main-first). Dropping the returned vector loses GC erases (dead-row leak) — hence
    //! [[nodiscard]].
    [[nodiscard]] virtual std::vector<IRetryableTransactionPtr> Sync(NApi::IDynamicTableTransactionPtr epochTransaction) = 0;

    virtual THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() = 0;
    virtual THashMap<TStreamId, std::pair<i64, i64>> GetCountAndByteSizes() = 0;
};

DEFINE_REFCOUNTED_TYPE(IOutputStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define OUTPUT_STORE_H_
#include "output_store-inl.h"
#undef OUTPUT_STORE_H_
