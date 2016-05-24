#pragma once

#include "public.h"

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
    const Stroka& GetSelfAddress() const;

    int GetQuorumCount() const;
    int GetPeerCount() const;

    const TNullable<Stroka>& GetPeerAddress(TPeerId id) const;
    NRpc::IChannelPtr GetPeerChannel(TPeerId id) const;

    const NProfiling::TTagIdList& GetPeerTags(TPeerId id) const;
    const NProfiling::TTagIdList& GetAllPeersTags() const;
    const NProfiling::TTagIdList& GetPeerQuorumTags() const;

    void Reconfigure(TCellConfigPtr newConfig);

    DEFINE_SIGNAL(void(TPeerId peerId), PeerReconfigured);

private:
    TCellConfigPtr Config;
    NRpc::IChannelFactoryPtr ChannelFactory;
    TPeerId SelfId;

    std::vector<NRpc::IChannelPtr> PeerChannels;

    std::vector<NProfiling::TTagIdList> PeerTags;
    NProfiling::TTagIdList AllPeersTags;
    NProfiling::TTagIdList PeerQuorumTags;

    NLogging::TLogger Logger;
         

    void BuildTags();

    NRpc::IChannelPtr CreatePeerChannel(TPeerId id);

};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

