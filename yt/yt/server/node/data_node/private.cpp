#include "private.h"

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger DataNodeLogger("DataNode");
const NProfiling::TRegistry DataNodeProfiler("/data_node");
const NProfiling::TRegistry LocationProfiler("/location");

const NLogging::TLogger P2PLogger("P2P");
const NProfiling::TRegistry P2PProfiler("/data_node/p2p");

const TString CellIdFileName("cell_id");
const TString LocationUuidFileName("uuid");
const TString MultiplexedDirectory("multiplexed");
const TString TrashDirectory("trash");
const TString CleanExtension("clean");
const TString SealedFlagExtension("sealed");
const TString ArtifactMetaSuffix(".artifact");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
