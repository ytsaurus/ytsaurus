#include "private.h"

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger DataNodeLogger("DataNode");
const NProfiling::TProfiler DataNodeProfiler("/data_node");

const Stroka CellIdFileName("cell_id");
const Stroka MultiplexedDirectory("multiplexed");
const Stroka TrashDirectory("trash");
const Stroka CleanExtension("clean");
const Stroka SealedFlagExtension("sealed");
const Stroka ArtifactMetaSuffix(".artifact");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
