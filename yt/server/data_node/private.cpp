#include "private.h"

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger DataNodeLogger("DataNode");
const NProfiling::TProfiler DataNodeProfiler("/data_node");
const NLogging::TLogger P2PLogger("P2P");
const NProfiling::TProfiler P2PProfiler("/p2p");

const TString CellIdFileName("cell_id");
const TString MultiplexedDirectory("multiplexed");
const TString TrashDirectory("trash");
const TString CleanExtension("clean");
const TString SealedFlagExtension("sealed");
const TString ArtifactMetaSuffix(".artifact");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
