#pragma once

#include "scheduler_base.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Executes actions in fiber context.
class TFiberScheduler
    : public TSchedulerThreadBase
{
public:
    TFiberScheduler(
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName);

    void CancelWait();
    void PrepareWait();
    void Wait();

    virtual TClosure BeginExecute() = 0;
    virtual void EndExecute() = 0;

private:
    std::optional<TEventCount::TCookie> Cookie_;

    virtual bool OnLoop(TEventCount::TCookie* cookie) override;
};

/////////////////////////////////////////////////////////////////////////////

// Temporary adapters.
using TSchedulerThread = TFiberScheduler;

DECLARE_REFCOUNTED_TYPE(TSchedulerThread)
DEFINE_REFCOUNTED_TYPE(TSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
