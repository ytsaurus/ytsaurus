#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct ITimers
    : public TRefCounted
{
    struct TTableKey
    {
        TComputationId ComputationId;
        TKey Key;
        TMessageId MessageId;
    };

    struct TFilter
    {
        std::optional<TComputationId> ComputationId;
        std::optional<TKey> LowerKey;
        std::optional<TKey> ExactKey;
        std::optional<TKey> UpperKey;
    };

    struct TLoadResult
    {
        std::vector<std::pair<TTableKey, TTimer>> Timers;
        std::optional<TTableKey> ContinuationOffsetExclusive;
    };

    virtual void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) = 0;

    virtual TFuture<TLoadResult> Load(
        TFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) = 0;

    virtual TFuture<std::vector<std::pair<TTableKey, TTimer>>> LoadAll(
        TFilter filter) = 0;

    virtual void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TTimer>& timers) = 0;

    virtual void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& tableKeys) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimers);

////////////////////////////////////////////////////////////////////////////////

class TTimers
    : public ITimers
{
public:
    TTimers(TContextPtr context, TDynamicTableRequestSpecPtr dynamicSpec);

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;

    TFuture<TLoadResult> Load(
        TFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<std::pair<TTableKey, TTimer>>> LoadAll(
        TFilter filter) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TTimer>& timers) override;

    void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& tableKeys) override;

private:
    struct TMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter SelectRows = Profiler.Counter("/select_rows");
        NProfiling::TCounter SelectBytes = Profiler.Counter("/select_bytes");
        NProfiling::TEventTimer SelectTime = Profiler.Timer("/select_time");
        NProfiling::TCounter WriteRows = Profiler.Counter("/write_rows");
        NProfiling::TCounter WriteBytes = Profiler.Counter("/write_bytes");
        NProfiling::TCounter EraseRows = Profiler.Counter("/erase_rows");
    };

    const TContextPtr Context_;
    TDynamicTableRequestSpecPtr DynamicSpec_;
    const NYPath::TYPath TablePath_;
    const NLogging::TLogger Logger;
    const std::string Tag_;
    const TMetrics Metrics_;
};

DEFINE_REFCOUNTED_TYPE(TTimers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
