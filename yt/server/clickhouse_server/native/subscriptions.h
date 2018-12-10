#pragma once

#include "public.h"

#include "directory.h"

#include <yt/ytlib/api/native/public.h>

namespace NYT::NClickHouseServer::NNative {

// TODO: split polling from subscription management

////////////////////////////////////////////////////////////////////////////////

struct ISubscriptionManager
    : public virtual TRefCounted
{
public:
    virtual void Subscribe(
        NApi::NNative::IClientPtr client,
        TString path,
        TNodeRevision expectedRevision,
        INodeEventHandlerWeakPtr eventHandler) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISubscriptionManager);

////////////////////////////////////////////////////////////////////////////////

ISubscriptionManagerPtr CreateSubscriptionManager();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
