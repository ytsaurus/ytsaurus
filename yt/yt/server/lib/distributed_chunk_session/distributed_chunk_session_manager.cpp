#include "distributed_chunk_session_manager.h"

namespace NYT::NDistributedChunkSession {

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionManagerPtr CreateDistributedChunkSessionManager(
    TDistributedChunkSessionServiceConfigPtr /*config*/,
    IInvokerPtr /*invoker*/,
    IConnectionPtr /*connection*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSession
