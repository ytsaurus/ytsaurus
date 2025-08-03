#pragma once

#include "fwd.h"
#include <yt/cpp/roren/interface/timers.h>

#include <library/cpp/yt/logging/logger.h>
#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/sensors_owner/sensors_owner.h>

#include <format>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class IExecutionContext
    : public NYT::TRefCounted
{
public:
    virtual ~IExecutionContext() = default;

    virtual TString GetExecutorName() const = 0;

    virtual NYT::NProfiling::TProfiler GetProfiler() const = 0;

    virtual const NYT::NProfiling::TSensorsOwner& GetSensorsOwner() const;

    virtual const NYT::NLogging::TLogger& GetLogger() const;

    virtual TInstant GetTime() const;

    virtual void SetTimer(const TTimer& timer, const TTimer::EMergePolicy policy) = 0;
    virtual void DeleteTimer(const TTimer::TKey& key) = 0;

    template <typename T>
    Y_FORCE_INLINE T* As()
    {
        auto casted = dynamic_cast<T*>(this);
        Y_ABORT_UNLESS(casted, "Trying to cast execution context for `%s` executor to incorrect type", GetExecutorName().c_str());
        return casted;
    }
};

DEFINE_REFCOUNTED_TYPE(IExecutionContext);

IExecutionContextPtr DummyExecutionContext();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
