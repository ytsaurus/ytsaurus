#include "stdafx.h"
#include "cell_manager.h"

#include <ytlib/misc/address.h>

#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NElection {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& SILENT_UNUSED Logger = ElectionLogger;

NRpc::TChannelCache TCellManager::ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(TCellConfigPtr config)
    : Config(config)
{
    OrderedAddresses = Config->Addresses;
    std::sort(OrderedAddresses.begin(), OrderedAddresses.end());
}

void TCellManager::Initialize()
{
    SelfAddress_ = BuildServiceAddress(TAddressResolver::Get()->GetLocalHostName(), Config->RpcPort);
    SelfId_ = std::distance(
        OrderedAddresses.begin(),
        std::find(OrderedAddresses.begin(), OrderedAddresses.end(), SelfAddress_));

    if (SelfId_ == OrderedAddresses.size()) {
        LOG_FATAL("Self is absent in the list of masters (SelfAddress: %s)",
            ~SelfAddress_);
    }
}

int TCellManager::GetQuorum() const
{
    return GetPeerCount() / 2 + 1;
}

int TCellManager::GetPeerCount() const
{
    return OrderedAddresses.size();
}

const Stroka& TCellManager::GetPeerAddress(TPeerId id) const
{
    return OrderedAddresses[id];
}

NRpc::IChannelPtr TCellManager::GetMasterChannel(TPeerId id) const
{
    return ChannelCache.GetChannel(GetPeerAddress(id));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
