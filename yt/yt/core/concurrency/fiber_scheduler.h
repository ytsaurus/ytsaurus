#pragma once

#include "scheduler_base.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Executes actions in fiber context.
class TFiberScheduler
    : public TSchedulerThreadBase
{
public:
    using TSchedulerThreadBase::TSchedulerThreadBase;

    void CancelWait();
    void PrepareWait();
    void Wait();

    virtual TClosure BeginExecute() = 0;
    virtual void EndExecute() = 0;

private:
    std::optional<NThreading::TEventCount::TCookie> Cookie_;

    bool OnLoop(NThreading::TEventCount::TCookie* cookie) override;
};

/////////////////////////////////////////////////////////////////////////////

// Temporary adapters.
using TSchedulerThread = TFiberScheduler;

DECLARE_REFCOUNTED_TYPE(TSchedulerThread)
DEFINE_REFCOUNTED_TYPE(TSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
