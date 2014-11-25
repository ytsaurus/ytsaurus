#include "stdafx.h"
#include "private.h"

#include <core/rpc/channel.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger DataNodeLogger("DataNode");
NProfiling::TProfiler DataNodeProfiler("/data_node");

NRpc::IChannelFactoryPtr ChannelFactory(NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

Stroka CellIdFileName("cell_id");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
