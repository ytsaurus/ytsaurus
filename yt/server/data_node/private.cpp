#include "private.h"

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/channel.h>

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
const Stroka HealthCheckFileName("health_check~");

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
