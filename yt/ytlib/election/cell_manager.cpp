#include "stdafx.h"
#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/address.h>

#include <ytlib/rpc/channel.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NElection {

///////////////////////////////////////////////////////////////////////////////

static auto& Logger = ElectionLogger;
static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(TCellConfigPtr config)
    : Config(config)
{ }

void TCellManager::Initialize()
{
    OrderedAddresses = Config->Addresses;
    std::sort(OrderedAddresses.begin(), OrderedAddresses.end());

    SelfAddress_ = BuildServiceAddress(
        TAddressResolver::Get()->GetLocalHostName(),
        Config->RpcPort);

    SelfId_ = std::distance(
        OrderedAddresses.begin(),
        std::find(OrderedAddresses.begin(), OrderedAddresses.end(), SelfAddress_));
    if (SelfId_ == OrderedAddresses.size()) {
        THROW_ERROR_EXCEPTION("Self address %s is missing in the cell members list",
            ~SelfAddress_.Quote());
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
