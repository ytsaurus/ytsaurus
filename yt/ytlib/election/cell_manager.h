#pragma once

#include "public.h"
#include "config.h"

#include <yt/core/actions/signal.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/property.h>

#include <yt/core/profiling/public.h>

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

    const TCellId& GetCellId() const;
    TPeerId GetSelfPeerId() const;
    const TCellPeerConfig& GetSelfConfig() const;

    int GetVotingPeerCount() const;
    int GetQuorumPeerCount() const;
    int GetTotalPeerCount() const;

    const TCellPeerConfig& GetPeerConfig(TPeerId id) const;
    NRpc::IChannelPtr GetPeerChannel(TPeerId id) const;

    NProfiling::TTagId GetPeerTag(TPeerId id) const;
    NProfiling::TTagId GetAllPeersTag() const;
    NProfiling::TTagId GetPeerQuorumTag() const;
    NProfiling::TTagId GetCellIdTag() const;

    void Reconfigure(TCellConfigPtr newConfig);

    DEFINE_SIGNAL(void(TPeerId peerId), PeerReconfigured);

private:
    TCellConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TPeerId SelfId_;

    const NLogging::TLogger Logger;

    int VotingPeerCount_;
    int QuorumPeerCount_;
    int TotalPeerCount_;

    std::vector<NRpc::IChannelPtr> PeerChannels_;

    NProfiling::TTagIdList PeerTags_;
    NProfiling::TTagId AllPeersTag_;
    NProfiling::TTagId PeerQuorumTag_;
    NProfiling::TTagId CellIdTag_;

    void BuildTags();
    NRpc::IChannelPtr CreatePeerChannel(TPeerId id);
};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

