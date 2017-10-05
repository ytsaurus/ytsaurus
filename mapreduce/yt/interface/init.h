#pragma once

#include <mapreduce/yt/interface/wait_proxy.h>

#include <util/generic/fwd.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TInitializeOptions
{
    using TSelf = TInitializeOptions;

    // Use blocking functions defined in the given IWaitProxy implementation or default ones
    FLUENT_FIELD_DEFAULT(::TIntrusivePtr<IWaitProxy>, WaitProxy, nullptr);
};

void Initialize(int argc, const char **argv, const TInitializeOptions &options = TInitializeOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
