#include "distributed_chunk_session_controller.h"

namespace NYT::NDistributedChunkSessionClient {

using namespace NObjectClient;

using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionControllerPtr CreateDistributedChunkSessionController(
    IClientPtr /*client*/,
    TDistributedChunkSessionControllerConfigPtr /*config*/,
    TTransactionId /*transactionId*/,
    std::vector<std::string> /*columns*/,
    IInvokerPtr /*invoker*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
