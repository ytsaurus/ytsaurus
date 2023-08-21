#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

struct IAlienCellPeerChannelFactory
    : public virtual TRefCounted
{
    virtual NRpc::IChannelPtr CreateChannel(
        const TString& cluster,
        TCellId cellId,
        int peerId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAlienCellPeerChannelFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election

