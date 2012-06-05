#pragma once

#include "common.h"
#include "config.h"

#include <ytlib/rpc/channel_cache.h>

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

    i32 GetQuorum() const;
    i32 GetPeerCount() const;
    Stroka GetPeerAddress(TPeerId id) const;

    template <class TProxy>
    TAutoPtr<TProxy> GetMasterProxy(TPeerId id) const;

private:
    TCellConfigPtr Config;
    std::vector<Stroka> OrderedAddresses;

    static NRpc::TChannelCache ChannelCache;

};

////////////////////////////////////////////////////////////////////////////////

template <class TProxy>
TAutoPtr<TProxy> TCellManager::GetMasterProxy(TPeerId id) const
{
    return new TProxy(ChannelCache.GetChannel(GetPeerAddress(id)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

