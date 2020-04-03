#include "cell_manager.h"
#include "private.h"
#include "config.h"

#include <yt/core/bus/client.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/client.h>

#include <yt/core/net/address.h>

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
    : Config_(std::move(config))
    , ChannelFactory_(std::move(channelFactory))
    , SelfId_(selfId)
    , VotingPeerCount_(Config_->CountVotingPeers())
    , QuorumPeerCount_(VotingPeerCount_ / 2 + 1)
    , TotalPeerCount_(Config_->Peers.size())
    , Logger(NLogging::TLogger(ElectionLogger)
        .AddTag("CellId: %v, SelfPeerId: %v",
            Config_->CellId,
            selfId))
{
    PeerChannels_.resize(TotalPeerCount_);
    for (TPeerId id = 0; id < TotalPeerCount_; ++id) {
        PeerChannels_[id] = CreatePeerChannel(Config_->Peers[id]);
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
