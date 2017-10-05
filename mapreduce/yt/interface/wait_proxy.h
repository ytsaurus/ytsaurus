#pragma once

#include <mapreduce/yt/interface/common.h>

#include <util/datetime/base.h>

namespace NThreading {
template <typename T>
class TFuture;
}

class Event;
class TCondVar;
class TMutex;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IWaitProxy
    : public TThrRefBase
{
public:
    virtual ~IWaitProxy() = default;

    virtual bool WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout) = 0;
    virtual bool WaitEvent(Event& event, TDuration timeout) = 0;
    virtual bool WaitCondVar(TCondVar& condVar, TMutex& mutex, TDuration timeout) = 0;
    virtual void Sleep(TDuration timeout) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
