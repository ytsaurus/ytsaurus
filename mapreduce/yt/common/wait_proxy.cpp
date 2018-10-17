#include "wait_proxy.h"


#include <library/threading/future/future.h>

#include <util/system/event.h>
#include <util/system/condvar.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

bool TDefaultWaitProxy::WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout)
{
    return future.Wait(timeout);
}

bool TDefaultWaitProxy::WaitEvent(TSystemEvent& event, TDuration timeout)
{
    return event.WaitT(timeout);
}

bool TDefaultWaitProxy::WaitCondVar(TCondVar &condVar, TMutex &mutex, TDuration timeout)
{
    return condVar.WaitT(mutex, timeout);
}

void TDefaultWaitProxy::Sleep(TDuration timeout)
{
    ::Sleep(timeout);
}

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IWaitProxy> &TWaitProxy::Get()
{
    static TIntrusivePtr<IWaitProxy> waitProxy = MakeIntrusive<TDefaultWaitProxy>();
    return waitProxy;
}

bool TWaitProxy::WaitFuture(const NThreading::TFuture<void>& future)
{
    return Get()->WaitFuture(future, TDuration::Max());
}

bool TWaitProxy::WaitFuture(const NThreading::TFuture<void>& future, TInstant deadLine)
{
    return Get()->WaitFuture(future, deadLine - TInstant::Now());
}

bool TWaitProxy::WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout)
{
    return Get()->WaitFuture(future, timeout);
}

bool TWaitProxy::WaitEventD(TSystemEvent& event, TInstant deadLine)
{
    return Get()->WaitEvent(event, deadLine - TInstant::Now());
}

bool TWaitProxy::WaitEventT(TSystemEvent& event, TDuration timeout)
{
    return Get()->WaitEvent(event, timeout);
}

void TWaitProxy::WaitEventI(TSystemEvent& event)
{
    Get()->WaitEvent(event, TDuration::Max());
}

bool TWaitProxy::WaitEvent(TSystemEvent& event)
{
    return Get()->WaitEvent(event, TDuration::Max());
}

bool TWaitProxy::WaitCondVarD(TCondVar& condVar, TMutex& m, TInstant deadLine)
{
    return Get()->WaitCondVar(condVar, m, deadLine - TInstant::Now());
}

bool TWaitProxy::WaitCondVarT(TCondVar& condVar, TMutex& m, TDuration timeOut)
{
    return Get()->WaitCondVar(condVar, m, timeOut);
}

void TWaitProxy::WaitCondVarI(TCondVar& condVar, TMutex& m)
{
    Get()->WaitCondVar(condVar, m, TDuration::Max());
}

void TWaitProxy::WaitCondVar(TCondVar& condVar, TMutex& m)
{
    Get()->WaitCondVar(condVar, m, TDuration::Max());
}

void TWaitProxy::Sleep(TDuration timeout)
{
    Get()->Sleep(timeout);
}

void TWaitProxy::SleepUntil(TInstant instant)
{
    Get()->Sleep(instant - TInstant::Now());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
