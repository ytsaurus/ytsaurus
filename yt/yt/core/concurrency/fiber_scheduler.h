#pragma once

#include "thread.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Executes actions in fiber context.
class TFiberScheduler
    : public TThread
{
public:
    TFiberScheduler(
        const TString& threadGroupName,
        const TString& threadName,
        int shutdownPriority);

    //! Empty callback signals about stopping.
    virtual TClosure OnExecute() = 0;

private:
    void ThreadMain() override;

    const TString ThreadGroupName_;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
