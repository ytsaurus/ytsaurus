#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/aggregate_property.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TLogicalTimeRegistry
    : public TRefCounted
{
public:
    TLogicalTimeRegistry(
        TLogicalTimeRegistryConfigPtr config,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        const NProfiling::TProfiler& profiler);

    class TLamportClock
    {
    public:
        DEFINE_BYVAL_RW_PROPERTY(TLogicalTime, Time);

        DEFINE_SIGNAL(void(TLogicalTime), Tick);

        TLogicalTime Tick(TLogicalTime externalTime = {});

        void Save(NHydra::TSaveContext& context) const;

        void Load(NHydra::TLoadContext& context);
    };

    TLamportClock* GetClock();

    std::pair<TLogicalTime, i64> GetConsistentState(std::optional<TLogicalTime> logicalTime);

private:
    const TLogicalTimeRegistryConfigPtr Config_;
    const IInvokerPtr AutomatonInvoker_;
    const NHydra::IHydraManagerPtr HydraManager_;

    TLamportClock Clock_;

    struct TTimeInfo
    {
        i64 SequenceNumber;
        TInstant Timestamp;
    };
    std::map<TLogicalTime, TTimeInfo> TimeInfoMap_;
    std::atomic<int> TimeInfoMapSize_;

    NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    void OnTick(TLogicalTime logicalTime);

    void OnEvict();
};

DEFINE_REFCOUNTED_TYPE(TLogicalTimeRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
