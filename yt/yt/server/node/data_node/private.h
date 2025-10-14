#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SessionIdAllocationTag = "session_id";

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DataNodeLogger, "DataNode");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, DataNodeProfiler, "/data_node");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, LocationProfiler, "/location");

YT_DEFINE_GLOBAL(const NLogging::TLogger, P2PLogger, "P2P");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, P2PProfiler, DataNodeProfiler().WithPrefix("/p2p"));

inline const TString MultiplexedDirectory("multiplexed");
inline const TString TrashDirectory("trash");
inline const TString CleanExtension("clean");
inline const TString SealedFlagExtension("sealed");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
