#include "stdafx.h"
#include "private.h"

#include <core/rpc/caching_bus_channel_factory-inl.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger DataNodeLogger("DataNode");
NProfiling::TProfiler DataNodeProfiler("/data_node");

struct TDataNodeTag;

NRpc::IChannelFactoryPtr GetDataNodeChannelFactory()
{
    return NRpc::GetCachingBusChannelFactory<TDataNodeTag>();
}

Stroka CellIdFileName("cell_id");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
