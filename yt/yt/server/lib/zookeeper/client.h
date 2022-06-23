#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestPtr>
struct TRequestContext
{
    TRequestPtr Request;
    TGuid RequestId;

    ISessionPtr Session;
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public TRefCounted
{
    virtual TRspPingPtr Ping(TRequestContext<TReqPingPtr> context) = 0;

    virtual TRspGetChildren2Ptr GetChildren2(TRequestContext<TReqGetChildren2Ptr> context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(NApi::IClientPtr ytClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
