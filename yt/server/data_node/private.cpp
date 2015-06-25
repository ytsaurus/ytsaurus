#include "stdafx.h"
#include "private.h"

#include <core/rpc/channel.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger DataNodeLogger("DataNode");
const NProfiling::TProfiler DataNodeProfiler("/data_node");

const NRpc::IChannelFactoryPtr ChannelFactory(NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

const Stroka CellIdFileName("cell_id");
const Stroka MultiplexedDirectory("multiplexed");
const Stroka TrashDirectory("trash");
const Stroka CleanExtension("clean");
const Stroka SealedFlagExtension("sealed");
const Stroka ArtifactMetaSuffix(".artifact");
const Stroka DisabledLockFileName("disabled");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
