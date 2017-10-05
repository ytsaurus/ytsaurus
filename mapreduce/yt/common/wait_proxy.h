#pragma once

#include <mapreduce/yt/interface/wait_proxy.h>


namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TDefaultWaitProxy
    : public IWaitProxy
{
public:
    TDefaultWaitProxy() = default;
    ~TDefaultWaitProxy() override = default;

    bool WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout) override;
    bool WaitEvent(Event& event, TDuration timeout) override;
    bool WaitCondVar(TCondVar& condVar, TMutex& mutex, TDuration timeout) override;
    void Sleep(TDuration timeout) override;
};

class TWaitProxy {
public:
    TWaitProxy() = delete;

    static TIntrusivePtr<IWaitProxy>& Get();

    static bool WaitFuture(const NThreading::TFuture<void>& future);
    static bool WaitFuture(const NThreading::TFuture<void>& future, TInstant deadLine);
    static bool WaitFuture(const NThreading::TFuture<void>& future, TDuration timeout);

    static bool WaitEventD(Event& event, TInstant deadLine);
    static bool WaitEventT(Event& event, TDuration timeout);
    static void WaitEventI(Event& event);
    static bool WaitEvent(Event& event);

    static bool WaitCondVarD(TCondVar& condVar, TMutex& m, TInstant deadLine);
    static bool WaitCondVarT(TCondVar& condVar, TMutex& m, TDuration timeOut);
    static void WaitCondVarI(TCondVar& condVar, TMutex& m);
    static void WaitCondVar(TCondVar& condVar, TMutex& m);

    static void Sleep(TDuration timeout);
    static void SleepUntil(TInstant instant);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
