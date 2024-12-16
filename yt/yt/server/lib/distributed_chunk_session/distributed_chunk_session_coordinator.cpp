#include "distributed_chunk_session_coordinator.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

namespace NYT::NDistributedChunkSession {

using namespace NChunkClient;
using namespace NNodeTrackerClient;

using NYT::ToProto;

using NApi::NNative::IConnectionPtr;
using NDistributedChunkSessionClient::NProto::TRspPingChunkSession;

////////////////////////////////////////////////////////////////////////////////

void ToProto(TRspPingChunkSession* proto, const TCoordinatorStatus& status)
{
    proto->set_close_demanded(status.CloseDemanded);
    proto->set_written_block_count(status.WrittenBlockCount);
    *proto->mutable_chunk_misc_meta() = status.ChunkMiscMeta;
    ToProto(proto->mutable_data_block_metas(), status.DataBlockMetas);
}

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionCoordinatorPtr CreateDistributedChunkSessionCoordinator(
    TDistributedChunkSessionServiceConfigPtr /*config*/,
    TSessionId /*sessionId*/,
    std::vector<TNodeDescriptor> /*targets*/,
    IInvokerPtr /*invoker*/,
    IConnectionPtr /*connection*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSession
