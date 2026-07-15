#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! IStatusErrorState is error state for some component. It is supposed to live as long as component lives.
//! If component stably fails, error state stably contains error and it can be observed from root status profiler.
//! If component is stably OK, error state is OK.
struct IStatusErrorState
    : public TRefCounted
{
    struct TStatus
    {
        TInstant LastOKTime;
        TInstant LastStateChangeTime;
        std::optional<bool> IsOK;
    };

    virtual void SetError(TError error) = 0;
    virtual void ClearError() = 0;

    virtual TStatus GetStatus() const = 0;
};

//! IStatusProfiler allows building a hierarchical tree of profilers and reporting component statuses.
//! Each component can create leaf nodes (IStatusErrorState) and keep its current error there.
//! The root IStatusProfiler can be queried to obtain an aggregated status for the entire tree.
//! When leaf nodes are destroyed, they are automatically removed from the tree.
//! Tree nodes keep their parents alive; therefore, as long as a leaf is alive, it remains visible
//! from the root.
struct IStatusProfiler
    : public TRefCounted
{
    struct TUnitedProfilerStatus
    {
        THashMap<std::string, TError> Errors; // From name with all prefixes to error.
    };

    virtual IStatusErrorStatePtr ErrorState(TStringBuf name) = 0;
    virtual IStatusProfilerPtr WithPrefix(TStringBuf prefix) = 0;
    virtual TUnitedProfilerStatus GetStatus() const = 0;
};

struct TStatusProfilerLoggingOptions
{
    //! Zero logs every change.
    TDuration LogThrottle = TDuration::Minutes(1);
    //! Zero disables the background report.
    TDuration ReportPeriod = TDuration::Minutes(1);
    TDuration StatusWindow = TDuration::Minutes(10);
};

//! Each registered component exports a #profiler gauge "/status_profiler/broken" tagged by its
//! path, 1 while the component is broken and 0 otherwise.
IStatusProfilerPtr CreateStatusProfiler(
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger,
    const TStatusProfilerLoggingOptions& options = {},
    const NProfiling::TProfiler& profiler = {});

//! Logs every change and runs no background report; for tests.
IStatusProfilerPtr CreateSyncStatusProfiler(const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
