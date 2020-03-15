#pragma once

#include "public.h"

#include <yt/core/actions/signal.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Thread affinity: single-threaded
 */
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

    void Reconfigure(const TCellConfigPtr& newConfig, TPeerId newSelfId);

    DEFINE_SIGNAL(void(TPeerId peerId), PeerReconfigured);

private:
    TCellConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    TPeerId SelfId_;

    NLogging::TLogger Logger;

    int VotingPeerCount_;
    int QuorumPeerCount_;
    int TotalPeerCount_;

    std::vector<NRpc::IChannelPtr> PeerChannels_;

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);

    void InitializeLogger();
    NRpc::IChannelPtr CreatePeerChannel(const TCellPeerConfig& peerConfig);
};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

