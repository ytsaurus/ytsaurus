#include "stdafx.h"
#include "cell_manager.h"

#include <ytlib/misc/address.h>

#include <ytlib/rpc/channel.h>

#include <ytlib/profiling/profiling_manager.h>

namespace NYT {
namespace NElection {

using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ElectionLogger;

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

    auto* profilingManager = NProfiling::TProfilingManager::Get();
    for (TPeerId id = 0; id < GetPeerCount(); ++id) {
        NProfiling::TTagIdList tags;
        tags.push_back(profilingManager->RegisterTag("address", TRawString(OrderedAddresses[id])));
        PeerTags.push_back(tags);
    }

    AllPeersTags.push_back(profilingManager->RegisterTag("address", TRawString("all")));
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
