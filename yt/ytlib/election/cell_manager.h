#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/actions/signal.h>

#include <core/rpc/public.h>

#include <core/logging/tagged_logger.h>

#include <core/profiling/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

TCellManagerPtr CreateCellManager(
    TCellConfigPtr config,
    TPeerId selfId);

class TCellManager
    : public TRefCounted
{
public:
    const TCellGuid& GetCellGuid() const;
    TPeerId GetSelfId() const;
    const Stroka& GetSelfAddress() const;

    int GetQuorumCount() const;
    int GetPeerCount() const;

    const Stroka& GetPeerAddress(TPeerId id) const;
    NRpc::IChannelPtr GetPeerChannel(TPeerId id) const;

    const NProfiling::TTagIdList& GetPeerTags(TPeerId id) const;
    const NProfiling::TTagIdList& GetAllPeersTags() const;
    const NProfiling::TTagIdList& GetPeerQuorumTags() const;

    void Reconfigure(TCellConfigPtr newConfig);

    DEFINE_SIGNAL(void(TPeerId peerId), PeerReconfigured);

private:
    template <class T, class... As>
    friend TIntrusivePtr<T> NYT::New(As&&...);

    TCellConfigPtr Config;
    TPeerId SelfId;

    std::vector<NRpc::IChannelPtr> PeerChannels;

    std::vector<NProfiling::TTagIdList> PeerTags;
    NProfiling::TTagIdList AllPeersTags;
    NProfiling::TTagIdList PeerQuorumTags;

    NLog::TTaggedLogger Logger;
         

    TCellManager(
        TCellConfigPtr config,
        TPeerId selfId);

    void BuildTags();

    NRpc::IChannelPtr CreatePeerChannel(TPeerId id);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

