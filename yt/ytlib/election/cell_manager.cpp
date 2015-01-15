#include "stdafx.h"
#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <core/misc/address.h>

#include <core/bus/config.h>
#include <core/bus/client.h>
#include <core/bus/tcp_client.h>

#include <core/rpc/helpers.h>

#include <core/profiling/profile_manager.h>

namespace NYT {
namespace NElection {

using namespace NYTree;
using namespace NBus;
using namespace NRpc;

///////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(
    TCellConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TPeerId selfId)
    : Config(config)
    , ChannelFactory(channelFactory)
    , SelfId(selfId)
    , Logger(ElectionLogger)
{
    BuildTags();

    PeerChannels.resize(GetPeerCount());
    for (TPeerId id = 0; id < GetPeerCount(); ++id) {
        if (id != selfId) {
            PeerChannels[id] = CreatePeerChannel(id);
        }
    }

    Logger.AddTag("CellId: %v", Config->CellId);

    LOG_INFO("Cell initialized (SelfId: %v, PeerAddresses: [%v])",
        SelfId,
        JoinToString(Config->Addresses));
}

void TCellManager::BuildTags()
{
    PeerTags.clear();
    auto* profilingManager = NProfiling::TProfileManager::Get();
    for (TPeerId id = 0; id < GetPeerCount(); ++id) {
        NProfiling::TTagIdList tags;
        tags.push_back(profilingManager->RegisterTag("address", GetPeerAddress(id)));
        PeerTags.push_back(tags);
    }

    AllPeersTags.clear();
    AllPeersTags.push_back(profilingManager->RegisterTag("address", "all"));
    
    PeerQuorumTags.clear();
    PeerQuorumTags.push_back(profilingManager->RegisterTag("address", "quorum"));
}

const TCellId& TCellManager::GetCellId() const
{
    return Config->CellId;
}

TPeerId TCellManager::GetSelfPeerId() const
{
    return SelfId;
}

const Stroka& TCellManager::GetSelfAddress() const
{
    return GetPeerAddress(GetSelfPeerId());
}

int TCellManager::GetQuorumCount() const
{
    return GetPeerCount() / 2 + 1;
}

int TCellManager::GetPeerCount() const
{
    return Config->Addresses.size();
}

const Stroka& TCellManager::GetPeerAddress(TPeerId id) const
{
    return Config->Addresses[id];
}

IChannelPtr TCellManager::GetPeerChannel(TPeerId id) const
{
    return PeerChannels[id];
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

void TCellManager::Reconfigure(TCellConfigPtr newConfig)
{
    if (Config->CellId != newConfig->CellId) {
        THROW_ERROR_EXCEPTION("Cannot change cell id from %v to %v",
            Config->CellId,
            newConfig->CellId);
    }

    auto addresses = Config->Addresses;
    auto newAddresses = newConfig->Addresses;

    if (addresses.size() != newAddresses.size()) {
        THROW_ERROR_EXCEPTION("Cannot change cell size from %v to %v",
            addresses.size(),
            newAddresses.size());
    }

    if (addresses[SelfId] != newAddresses[SelfId]) {
        THROW_ERROR_EXCEPTION("Cannot change self address from %Qv to %Qv",
            addresses[SelfId],
            newAddresses[SelfId]);
    }

    BuildTags();
    Config = newConfig;

    for (TPeerId id = 0; id < GetPeerCount(); ++id) {
        if (addresses[id] != newAddresses[id]) {
            LOG_INFO("Peer %v reconfigured: %v -> %v",
                id,
                addresses[id],
                newAddresses[id]);
            PeerChannels[id] = CreatePeerChannel(id);
            PeerReconfigured_.Fire(id);
        }
    }
}

IChannelPtr TCellManager::CreatePeerChannel(TPeerId id)
{
    return CreateRealmChannel(
        ChannelFactory->CreateChannel(GetPeerAddress(id)),
        Config->CellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
