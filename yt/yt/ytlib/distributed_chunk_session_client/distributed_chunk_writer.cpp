#include "distributed_chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NNodeTrackerClient;

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkWriterPtr CreateDistributedChunkWriter(
    TNodeDescriptor /*coordinator*/,
    TSessionId /*sessionId*/,
    IConnectionPtr /*connection*/,
    TDistributedChunkWriterConfigPtr /*config*/,
    IInvokerPtr /*invoker*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
