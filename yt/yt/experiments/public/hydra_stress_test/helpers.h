#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NHydraStressTest {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NYT::NLogging::TLogger, HydraStressTestLogger, "HydraStressTest");

using TValue = i64;

inline TString GetPeerAddress(int peerId)
{
    return NYT::Format("peer-%v:22", peerId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest

