#pragma once

#include "common.h"
#include "config.h"

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/profiling/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager
    : public TRefCounted
{
public:
    explicit TCellManager(TCellConfigPtr config);

    DEFINE_BYVAL_RO_PROPERTY(TPeerId, SelfId);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, SelfAddress);

    void Initialize();

    int GetQuorum() const;
    int GetPeerCount() const;
    const Stroka& GetPeerAddress(TPeerId id) const;
    NRpc::IChannelPtr GetMasterChannel(TPeerId id) const;

    const NProfiling::TTagIdList& GetPeerTags(TPeerId id) const;
    const NProfiling::TTagIdList& GetAllPeersTags() const;
    const NProfiling::TTagIdList& GetPeerQuorumTags() const;

private:
    TCellConfigPtr Config;
    std::vector<Stroka> OrderedAddresses;

    std::vector<NProfiling::TTagIdList> PeerTags;
    NProfiling::TTagIdList AllPeersTags;
    NProfiling::TTagIdList PeerQuorumTags;

    static NRpc::TChannelCache ChannelCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

