#pragma once

#include "alien_cell_peer_channel_factory.h"
#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager
    : public TRefCounted
{
public:
    TCellManager(
        TCellConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        IAlienCellPeerChannelFactoryPtr alienChannelFactory,
        TPeerId selfId);

    TCellId GetCellId() const;
    TPeerId GetSelfPeerId() const;
    const TCellPeerConfigPtr& GetSelfConfig() const;

    int GetVotingPeerCount() const;
    int GetQuorumPeerCount() const;
    int GetTotalPeerCount() const;

    const TCellPeerConfigPtr& GetPeerConfig(TPeerId id) const;
    NRpc::IChannelPtr GetPeerChannel(TPeerId id) const;

private:
    const TCellConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const IAlienCellPeerChannelFactoryPtr AlienCellPeerChannelFactory_;
    const TPeerId SelfId_;

    const int VotingPeerCount_;
    const int QuorumPeerCount_;
    const int TotalPeerCount_;

    const NLogging::TLogger Logger;

    std::vector<NRpc::IChannelPtr> PeerChannels_;

    NRpc::IChannelPtr CreatePeerChannel(TPeerId id, const TCellPeerConfigPtr& peerConfig);
};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

