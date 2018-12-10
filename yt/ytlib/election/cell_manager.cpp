#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <yt/core/bus/client.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/client.h>

#include <yt/core/net/address.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/helpers.h>

namespace NYT::NElection {

using namespace NYTree;
using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(
    TCellConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TPeerId selfId)
    : Config_(config)
    , ChannelFactory_(std::move(channelFactory))
    , SelfId_(selfId)
    , Logger(NLogging::TLogger(ElectionLogger)
        .AddTag("CellId: %v", Config_->CellId))
{
    TotalPeerCount_ = config->Peers.size();
    VotingPeerCount_ = 0;
    for (const auto& peer : Config_->Peers) {
        if (peer.Voting) {
            ++VotingPeerCount_;
        }
    }
    QuorumPeerCount_ = VotingPeerCount_ / 2 + 1;

    BuildTags();

    PeerChannels_.resize(TotalPeerCount_);
    for (TPeerId id = 0; id < TotalPeerCount_; ++id) {
        PeerChannels_[id] = CreatePeerChannel(id);
    }

    LOG_INFO("Cell initialized (SelfId: %v, Peers: %v)",
        SelfId_,
        Config_->Peers);
}

void TCellManager::BuildTags()
{
    PeerTags_.clear();
    auto* profilingManager = NProfiling::TProfileManager::Get();
    for (TPeerId id = 0; id < GetTotalPeerCount(); ++id) {
        const auto& config = GetPeerConfig(id);
        PeerTags_.push_back(
            config.Address
            ? profilingManager->RegisterTag("address", *config.Address)
            : -1);
    }

    AllPeersTag_ = profilingManager->RegisterTag("address", "all");
    PeerQuorumTag_ = profilingManager->RegisterTag("address", "quorum");
    CellIdTag_ = profilingManager->RegisterTag("cell_id", Config_->CellId);
}

const TCellId& TCellManager::GetCellId() const
{
    return Config_->CellId;
}

TPeerId TCellManager::GetSelfPeerId() const
{
    return SelfId_;
}

const TCellPeerConfig& TCellManager::GetSelfConfig() const
{
    return GetPeerConfig(GetSelfPeerId());
}

int TCellManager::GetVotingPeerCount() const
{
    return VotingPeerCount_;
}

int TCellManager::GetQuorumPeerCount() const
{
    return QuorumPeerCount_;
}

int TCellManager::GetTotalPeerCount() const
{
    return TotalPeerCount_;
}

const TCellPeerConfig& TCellManager::GetPeerConfig(TPeerId id) const
{
    return Config_->Peers[id];
}

IChannelPtr TCellManager::GetPeerChannel(TPeerId id) const
{
    return PeerChannels_[id];
}

NProfiling::TTagId TCellManager::GetPeerTag(TPeerId id) const
{
    auto tag = PeerTags_[id];
    YCHECK(tag != -1);
    return tag;
}

NProfiling::TTagId TCellManager::GetAllPeersTag() const
{
    return AllPeersTag_;
}

NProfiling::TTagId TCellManager::GetPeerQuorumTag() const
{
    return PeerQuorumTag_;
}

NProfiling::TTagId TCellManager::GetCellIdTag() const
{
    return CellIdTag_;
}

void TCellManager::Reconfigure(TCellConfigPtr newConfig)
{
    if (Config_->CellId != newConfig->CellId) {
        THROW_ERROR_EXCEPTION("Cannot change cell id from %v to %v",
            Config_->CellId,
            newConfig->CellId);
    }

    if (Config_->Peers.size() != newConfig->Peers.size()) {
        THROW_ERROR_EXCEPTION("Cannot change cell size from %v to %v",
            Config_->Peers.size(),
            newConfig->Peers.size());
    }

    const auto& newSelfPeer = newConfig->Peers[SelfId_];
    const auto& oldSelfPeer = Config_->Peers[SelfId_];
    if (newSelfPeer.Address != oldSelfPeer.Address) {
        THROW_ERROR_EXCEPTION("Cannot change self address from %Qv to %Qv",
            oldSelfPeer.Address,
            newSelfPeer.Address);
    }

    auto oldConfig = std::move(Config_);
    Config_ = std::move(newConfig);

    BuildTags();

    const auto& newPeers = Config_->Peers;
    const auto& oldPeers = oldConfig->Peers;
    for (TPeerId id = 0; id < GetTotalPeerCount(); ++id) {
        const auto& newPeer = newPeers[id];
        const auto& oldPeer = oldPeers[id];
        if (newPeer.Address != oldPeer.Address || newPeer.Voting != oldPeer.Voting) {
            LOG_INFO("Peer reconfigured (PeerId: %v, Address: %v -> %v, Voting: %v -> %v)",
                id,
                oldPeer.Address,
                newPeer.Address,
                oldPeer.Voting,
                newPeer.Voting);
            PeerChannels_[id] = CreatePeerChannel(id);
            PeerReconfigured_.Fire(id);
        }
    }
}

IChannelPtr TCellManager::CreatePeerChannel(TPeerId id)
{
    const auto& config = GetPeerConfig(id);
    if (!config.Address) {
        return nullptr;
    }
    return CreateRealmChannel(
        ChannelFactory_->CreateChannel(*config.Address),
        Config_->CellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
