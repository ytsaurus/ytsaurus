#include "message_distributor_detail.h"

namespace NYT::NFlow::NWorker {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

IMessageDistributorPtr CreateMessageDistributor(
    IJobDirectoryPtr jobDirectory,
    IChannelFactoryPtr channelFactory,
    TStreamSpecStoragePtr streamSpecStorage)
{
    const auto distributor = New<TMessageDistributor>(
        std::move(jobDirectory),
        std::move(channelFactory),
        std::move(streamSpecStorage));
    distributor->Initialize();
    return distributor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
