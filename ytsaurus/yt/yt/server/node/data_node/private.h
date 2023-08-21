#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger DataNodeLogger("DataNode");
inline const NProfiling::TProfiler DataNodeProfiler("/data_node");
inline const NProfiling::TProfiler LocationProfiler("/location");

inline const NLogging::TLogger P2PLogger("P2P");
inline const NProfiling::TProfiler P2PProfiler = DataNodeProfiler.WithPrefix("/p2p");

inline const TString CellIdFileName("cell_id");
inline const TString ChunkLocationUuidFileName("uuid");
inline const TString ChunkLocationUuidResetFileName("uuid_reset");
inline const TString MultiplexedDirectory("multiplexed");
inline const TString TrashDirectory("trash");
inline const TString CleanExtension("clean");
inline const TString SealedFlagExtension("sealed");
inline const TString ArtifactMetaSuffix(".artifact");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
