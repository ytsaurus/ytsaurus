#pragma once

#include "public.h"

#include <yt/core/actions/signal.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager
    : public TRefCounted
{
public:
    TCellManager(
        TCellConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        TPeerId selfId);

    TCellId GetCellId() const;
    TPeerId GetSelfPeerId() const;
    const TCellPeerConfig& GetSelfConfig() const;

    int GetVotingPeerCount() const;
    int GetQuorumPeerCount() const;
    int GetTotalPeerCount() const;

    const TCellPeerConfig& GetPeerConfig(TPeerId id) const;
    NRpc::IChannelPtr GetPeerChannel(TPeerId id) const;

private:
    const TCellConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TPeerId SelfId_;

    const int VotingPeerCount_;
    const int QuorumPeerCount_;
    const int TotalPeerCount_;

    const NLogging::TLogger Logger;

    std::vector<NRpc::IChannelPtr> PeerChannels_;

    NRpc::IChannelPtr CreatePeerChannel(const TCellPeerConfig& peerConfig);
};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

