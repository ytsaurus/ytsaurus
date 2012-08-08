#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger DataNodeLogger("DataNode");
NProfiling::TProfiler DataNodeProfiler("/data_node");
NRpc::TChannelCache ChannelCache;
Stroka CellGuidFileName("cell_guid");

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
