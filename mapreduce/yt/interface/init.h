#pragma once

#include <mapreduce/yt/interface/wait_proxy.h>

#include <util/generic/fwd.h>

#include <functional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TInitializeOptions
{
    using TSelf = TInitializeOptions;

    // Use blocking functions defined in the given IWaitProxy implementation or default ones
    FLUENT_FIELD_DEFAULT(::TIntrusivePtr<IWaitProxy>, WaitProxy, nullptr);

    // Abort all running transactions and watched operations when program terminates on error or signal
    FLUENT_FIELD_DEFAULT(bool, CleanupOnTermination, false);

    // `JobOnExitFunction' will be called just before exit() when program is started in job mode,
    //  might be useful for shutdowning libraries that are used inside operations.
    //
    // NOTE: Keep in mind that inside job execution environment differs from client execution environment.
    // So JobOnExitFunction should not depend on argc/argv environment variables etc.
    FLUENT_FIELD_OPTION(std::function<void()>, JobOnExitFunction);
};

void Initialize(int argc, const char **argv, const TInitializeOptions &options = TInitializeOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
