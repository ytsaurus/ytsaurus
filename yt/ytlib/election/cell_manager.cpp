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
    VotingPeerCount_ = Config_->CountVotingPeers();
    QuorumPeerCount_ = VotingPeerCount_ / 2 + 1;

    BuildTags();

    PeerChannels_.resize(TotalPeerCount_);
    for (TPeerId id = 0; id < TotalPeerCount_; ++id) {
        PeerChannels_[id] = CreatePeerChannel(Config_->Peers[id]);
    }

    YT_LOG_INFO("Cell initialized (SelfId: %v, Peers: %v, VotingPeers: %v)",
        SelfId_,
        Config_->Peers,
        VotingPeerCount_);
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

TCellId TCellManager::GetCellId() const
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
    YT_VERIFY(tag != -1);
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

void TCellManager::Reconfigure(TCellConfigPtr newConfig, TPeerId selfId)
{
    if (Config_->CellId != newConfig->CellId) {
        THROW_ERROR_EXCEPTION("Cannot change cell id from %v to %v",
            Config_->CellId,
            newConfig->CellId);
    }

    if (newConfig->CountVotingPeers() != VotingPeerCount_) {
        THROW_ERROR_EXCEPTION("Cannot change number of the voting peers from %v to %v",
            VotingPeerCount_,
            newConfig->CountVotingPeers());
    }

    if (VotingPeerCount_ > 1) {
        if (selfId != SelfId_) {
            THROW_ERROR_EXCEPTION("Cannot change self id from %v to %v since there are %v voting peers",
                SelfId_,
                selfId,
                VotingPeerCount_);
        }
        if (newConfig->Peers.size() != Config_->Peers.size()) {
            THROW_ERROR_EXCEPTION("Cannot change cell size from %v to %v since there are %v voting peers",
                Config_->Peers.size(),
                newConfig->Peers.size(),
                VotingPeerCount_);
        }
    }

    auto oldConfig = std::move(Config_);
    Config_ = std::move(newConfig);

    const auto& newPeers = Config_->Peers;
    const auto& oldPeers = oldConfig->Peers;

    if (selfId != SelfId_) {
        PeerReconfigured_.Fire(SelfId_);
        YT_LOG_DEBUG("Peer self id changed (Address: %v, SelfId %v -> %v)",
            oldPeers[SelfId_].Address,
            SelfId_,
            selfId);
        SelfId_ = selfId;
    }

    TotalPeerCount_ = static_cast<int>(newPeers.size());

    PeerChannels_.resize(std::max(newPeers.size(), oldPeers.size()));

    for (TPeerId id = 0; id < PeerChannels_.size(); ++id) {
        if (id >= oldPeers.size() && id < newPeers.size()) {
            YT_LOG_INFO("Peer created (PeerId: %v, Address: %v, Voting: %v)",
                id,
                newPeers[id].Address,
                newPeers[id].Voting);
            PeerChannels_[id] = CreatePeerChannel(newPeers[id]);
            PeerReconfigured_.Fire(id);
        } else if (id < oldPeers.size() && id >= newPeers.size()) {
            YT_LOG_INFO("Peer removed (PeerId: %v, Address: %v, Voting: %v)",
                id,
                oldPeers[id].Address,
                oldPeers[id].Voting);
            PeerReconfigured_.Fire(id);
        } else {
            YT_VERIFY(id < oldPeers.size() && id < newPeers.size());
            const auto& newPeer = newPeers[id];
            const auto& oldPeer = oldPeers[id];
            if (newPeer.Address != oldPeer.Address || newPeer.Voting != oldPeer.Voting) {
                YT_LOG_INFO("Peer reconfigured (PeerId: %v, Address: %v -> %v, Voting: %v -> %v)",
                    id,
                    oldPeer.Address,
                    newPeer.Address,
                    oldPeer.Voting,
                    newPeer.Voting);
                PeerChannels_[id] = CreatePeerChannel(newPeers[id]);
                PeerReconfigured_.Fire(id);
            }
        }
    }

    PeerChannels_.resize(newPeers.size());

    if (oldPeers.size() != newPeers.size()) {
        YT_LOG_INFO("Peer count changed (OldPeerCount: %v, NewPeerCount: %v)",
            oldPeers.size(),
            newPeers.size());
    }

    BuildTags();
}

IChannelPtr TCellManager::CreatePeerChannel(const TCellPeerConfig& config)
{
    if (!config.Address) {
        return nullptr;
    }
    return CreateRealmChannel(
        ChannelFactory_->CreateChannel(*config.Address),
        Config_->CellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
