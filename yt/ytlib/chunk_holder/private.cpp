#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger DataNodeLogger("DataNode");
NProfiling::TProfiler DataNodeProfiler("/data_node");
NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
