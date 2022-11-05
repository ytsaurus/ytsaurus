#pragma once

#include "thread.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Executes actions in fiber context.
class TFiberSchedulerThread
    : public TThread
{
public:
    TFiberSchedulerThread(
        const TString& threadGroupName,
        const TString& threadName,
        EThreadPriority threadPriority = EThreadPriority::Normal,
        int shutdownPriority = 0);

    //! Empty callback signals about stopping.
    virtual TClosure OnExecute() = 0;

private:
    void ThreadMain() override;

    const TString ThreadGroupName_;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
