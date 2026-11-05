#pragma once

#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>
#include <yql/essentials/utils/runnable.h>

#include <util/generic/string.h>

namespace NYql::NFmr {

struct TVanillaHttpMonSettings {
    TString Host;
    ui16 Port = 8003;
};

IRunnable::TPtr MakeVanillaHttpMon(
    IVanillaPeerTracker* tracker,
    const TVanillaHttpMonSettings& settings
);

} // namespace NYql::NFmr
