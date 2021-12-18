#pragma once

#include "fiber_scheduler.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerThreadBase)

class TSchedulerThreadBase
    : public TFiberScheduler
{
public:
    ~TSchedulerThreadBase();

    void Stop(bool graceful);

    void Stop();

protected:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;
    std::atomic<bool> GracefulStop_ = false;

    TSchedulerThreadBase(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        int shutdownPriority = 0);

    virtual void OnStart();
    virtual void OnStop();

private:
    void StartEpilogue() override;
    void StopPrologue() override;
    void StopEpilogue() override;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerThreadBase)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThread
    : public TSchedulerThreadBase
{
protected:
    using TSchedulerThreadBase::TSchedulerThreadBase;

    TClosure OnExecute() override;

    virtual TClosure BeginExecute() = 0;
    virtual void EndExecute() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
