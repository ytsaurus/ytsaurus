#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <array>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Per-computation companion metrics shared by all hosted jobs.
class TComputationCounters
    : public TRefCounted
{
public:
    explicit TComputationCounters(NProfiling::TProfiler profiler);

    //! ProcessBatch count, size, wall time, and CPU time.
    NProfiling::TCounter RequestCount;
    NProfiling::TSummary RequestSize;
    NProfiling::TEventTimer RequestDuration;
    NProfiling::TTimeCounter RequestCpuTime;

    //! Batches carrying job info that attempt local job recreation.
    NProfiling::TCounter JobRecreationCount;

    //! Records ProcessBatch outcomes by |status|, including RPC errors.
    void ProfileResponse(NProto::NCompanion::EResponseStatus status);

    //! Returns the serialized-size summary for one state dimension.
    NProfiling::TSummary GetStateSizeSummary(
        TStringBuf direction,
        TStringBuf stateType,
        const std::string& stateName);

private:
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StateSizesLock_);
    //! State-size summaries keyed by direction, type, and name.
    THashMap<std::string, NProfiling::TSummary> StateSizes_;

    static constexpr int ResponseStatusCount = NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED + 1;
    std::array<NProfiling::TCounter, ResponseStatusCount> ResponseCounts_;
};

DEFINE_REFCOUNTED_TYPE(TComputationCounters);

////////////////////////////////////////////////////////////////////////////////

//! Companion-specific and per-computation views beyond native RPC metrics.
class TCompanionProfiler
    : public TRefCounted
{
public:
    //! Publishes companion-local metrics; |registry| defaults to the process-wide registry.
    explicit TCompanionProfiler(
        const TJobRegistryPtr& jobRegistry,
        const NProfiling::TSolomonRegistryPtr& registry = nullptr);

    TComputationCountersPtr GetComputationCounters(const TComputationId& computationId);

    //! Profiler for a hosted computation, preserving in-worker metric names.
    NProfiling::TProfiler GetComputationProfiler(const TComputationId& computationId) const;

    void ProfileResourceExecute(
        NCompanion::ECompanionResourceCommand command,
        NCompanion::ECompanionResourceExecuteStatus status);

private:
    const NProfiling::TProfiler Profiler_;
    const NProfiling::TProfiler ComputationProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TComputationId, TComputationCountersPtr> ComputationCounters_;
    //! Keyed by "<command>/<status>"; both are enums, so the map stays tiny.
    THashMap<std::string, NProfiling::TCounter> ResourceExecuteCounters_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionProfiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
