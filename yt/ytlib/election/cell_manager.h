#pragma once

#include "public.h"
#include "config.h"

#include <yt/core/actions/signal.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/property.h>

#include <yt/core/profiling/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NElection {

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

    const NProfiling::TTagIdList& GetPeerTags(TPeerId id) const;
    const NProfiling::TTagIdList& GetAllPeersTags() const;
    const NProfiling::TTagIdList& GetPeerQuorumTags() const;
    const NProfiling::TTagIdList& GetCellIdTags() const;

    void Reconfigure(TCellConfigPtr newConfig);

    DEFINE_SIGNAL(void(TPeerId peerId), PeerReconfigured);

private:
    TCellConfigPtr Config_;
    NRpc::IChannelFactoryPtr ChannelFactory_;
    TPeerId SelfId_;

    int VotingPeerCount_;
    int QuorumPeerCount_;
    int TotalPeerCount_;

    std::vector<NRpc::IChannelPtr> PeerChannels_;

    std::vector<NProfiling::TTagIdList> PeerTags_;
    NProfiling::TTagIdList AllPeersTags_;
    NProfiling::TTagIdList PeerQuorumTags_;
    NProfiling::TTagIdList CellIdTags_;

    NLogging::TLogger Logger;
         

    void BuildTags();

    NRpc::IChannelPtr CreatePeerChannel(TPeerId id);

};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

