#include "stdafx.h"
#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <core/misc/address.h>

#include <core/bus/config.h>
#include <core/bus/client.h>
#include <core/bus/tcp_client.h>

#include <core/rpc/helpers.h>

#include <core/profiling/profiling_manager.h>

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

    Logger.AddTag("CellGuid: %v", Config->CellGuid);

    LOG_INFO("Cell initialized (SelfId: %v, PeerAddresses: [%v])",
        SelfId,
        JoinToString(Config->Addresses));
}

void TCellManager::BuildTags()
{
    PeerTags.clear();
    auto* profilingManager = NProfiling::TProfilingManager::Get();
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

const TCellGuid& TCellManager::GetCellGuid() const
{
    return Config->CellGuid;
}

TPeerId TCellManager::GetSelfId() const
{
    return SelfId;
}

const Stroka& TCellManager::GetSelfAddress() const
{
    return GetPeerAddress(GetSelfId());
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
    if (Config->CellGuid != newConfig->CellGuid) {
        THROW_ERROR_EXCEPTION("Cannot change cell GUID from %v to %v",
            Config->CellGuid,
            newConfig->CellGuid);
    }

    auto addresses = Config->Addresses;
    auto newAddresses = newConfig->Addresses;

    if (addresses.size() != newAddresses.size()) {
        THROW_ERROR_EXCEPTION("Cannot change cell size from %" PRISZT " to %" PRISZT,
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
        Config->CellGuid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
