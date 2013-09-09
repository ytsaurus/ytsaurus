#include "stdafx.h"
#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <core/misc/address.h>

#include <core/rpc/channel.h>
#include <core/rpc/channel_cache.h>

#include <core/profiling/profiling_manager.h>

namespace NYT {
namespace NElection {

using namespace NYTree;

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

    auto* profilingManager = NProfiling::TProfilingManager::Get();
    for (TPeerId id = 0; id < GetPeerCount(); ++id) {
        NProfiling::TTagIdList tags;
        tags.push_back(profilingManager->RegisterTag("address", OrderedAddresses[id]));
        PeerTags.push_back(tags);
    }

    AllPeersTags.push_back(profilingManager->RegisterTag("address", "all"));
    PeerQuorumTags.push_back(profilingManager->RegisterTag("address", "quorum"));
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

const NProfiling::TTagIdList& TCellManager::GetPeerTags(TPeerId id) const
{
    return PeerTags[id];
}

const NProfiling::TTagIdList& TCellManager::GetAllPeersTags() const
{
    return AllPeersTags;
}

const NProfiling::TTagIdList& TCellManager::GetPeerQuorumTags() const
{
    return PeerQuorumTags;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
