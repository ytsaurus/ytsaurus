#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Singleton class which can register TCallback signal handlers.
class TSignalRegistry
{
public:
    static TSignalRegistry* Get();

    void RegisterHandler(int signal, TCallback<void(void)> callback);

private:
    TCallback<void(void)> Handlers_[64];

    static void Handle(int signal);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
