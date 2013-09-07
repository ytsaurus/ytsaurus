#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger DataNodeLogger("DataNode");
NProfiling::TProfiler DataNodeProfiler("/data_node");
NRpc::TChannelCache ChannelCache;
Stroka CellGuidFileName("cell_guid");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
