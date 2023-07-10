#pragma once

#include "fwd.h"
#include <yt/cpp/roren/library/timers/timers.h>
#include <yt/cpp/roren/interface/timers.h>

#include <yt/yt/library/profiling/sensor.h>

#include <format>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class IRawTimerFn;

class IExecutionContext
    : public TThrRefBase
{
public:
    virtual ~IExecutionContext() = default;

    virtual TString GetExecutorName() const = 0;

    virtual NYT::NProfiling::TProfiler GetProfiler() const = 0;

    virtual void SetTimer(const TTimer& timer, const TTimer::EMergePolicy policy) = 0;
    virtual void DeleteTimer(const TTimer::TKey& key) = 0;

    template <typename T>
    Y_FORCE_INLINE T* As()
    {
        auto casted = dynamic_cast<T*>(this);
        Y_VERIFY(casted, "Trying to cast execution context for `%s` executor to incorrect type", GetExecutorName().c_str());
        return casted;
    }
};

IExecutionContextPtr DummyExecutionContext();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
