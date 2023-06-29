#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum ETabletState
{
    TS_MOUNTED /* "mounted" */,
    TS_UNMOUNTED /* "unmounted" */,
    TS_FROZEN /* "frozen" */,
};

struct TWaitForTabletsStateOptions
{
    using TSelf = TWaitForTabletsStateOptions;

    // Total waiting timeout. By default timeout is disabled.
    FLUENT_FIELD_DEFAULT(TDuration, Timeout, TDuration::Max());

    // How often should we check for tablets state.
    FLUENT_FIELD_DEFAULT(TDuration, CheckInterval, TDuration::Seconds(5));
};

//
// Helper methods that wait until all tablets are in specified state.
// Throw exception if timeout exceeded and some tablets are still not in specified state.
void WaitForTabletsState(const IClientPtr& client, const TYPath& table, ETabletState tabletState,
    const TWaitForTabletsStateOptions& options = TWaitForTabletsStateOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
