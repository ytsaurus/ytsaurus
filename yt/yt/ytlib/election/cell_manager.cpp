#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <yt/yt/core/bus/client.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NElection {

using namespace NYTree;
using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(
    TCellConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IAlienCellPeerChannelFactoryPtr alienChannelFactory,
    int selfId)
    : Config_(std::move(config))
    , ChannelFactory_(std::move(channelFactory))
    , AlienCellPeerChannelFactory_(std::move(alienChannelFactory))
    , SelfId_(selfId)
    , VotingPeerCount_(Config_->CountVotingPeers())
    , QuorumPeerCount_(VotingPeerCount_ / 2 + 1)
    , TotalPeerCount_(Config_->Peers.size())
    , Logger(ElectionLogger.WithTag("CellId: %v, SelfPeerId: %v",
        Config_->CellId,
        selfId))
{
    PeerChannels_.resize(TotalPeerCount_);
    for (int id = 0; id < TotalPeerCount_; ++id) {
        PeerChannels_[id] = CreatePeerChannel(id, Config_->Peers[id]);
    }

    YT_LOG_INFO("Cell initialized (SelfId: %v, Peers: %v, VotingPeers: %v)",
        SelfId_,
        Config_->Peers,
        VotingPeerCount_);
}

TCellId TCellManager::GetCellId() const
{
    return Config_->CellId;
}

int TCellManager::GetSelfPeerId() const
{
    return SelfId_;
}

const TCellPeerConfigPtr& TCellManager::GetSelfConfig() const
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

const TCellPeerConfigPtr& TCellManager::GetPeerConfig(int id) const
{
    return Config_->Peers[id];
}

IChannelPtr TCellManager::GetPeerChannel(int id) const
{
    return PeerChannels_[id];
}

IChannelPtr TCellManager::CreatePeerChannel(int id, const TCellPeerConfigPtr& config)
{
    if (config->AlienCluster) {
        return AlienCellPeerChannelFactory_->CreateChannel(
            *config->AlienCluster,
            Config_->CellId,
            id);
    }

    if (!config->Address) {
        return nullptr;
    }

    return CreateRealmChannel(
        ChannelFactory_->CreateChannel(*config->Address),
        Config_->CellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
