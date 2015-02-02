#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/actions/signal.h>

#include <core/rpc/public.h>

#include <core/logging/log.h>

#include <core/profiling/public.h>

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

    NLog::TLogger Logger;
         

    void BuildTags();

    NRpc::IChannelPtr CreatePeerChannel(TPeerId id);

};

DEFINE_REFCOUNTED_TYPE(TCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

