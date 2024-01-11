#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

static NYT::NLogging::TLogger HydraStressTestLogger("HydraStressTest");

using TValue = i64;

inline TString GetPeerAddress(int peerId)
{
    return NYT::Format("peer-%v", peerId);
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest

