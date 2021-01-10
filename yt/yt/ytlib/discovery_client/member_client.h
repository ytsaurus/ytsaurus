#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/attributes.h>

#include <yt/core/rpc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

struct IMemberClient
    : public virtual TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(TMemberClientConfigPtr config) = 0;

    virtual NYTree::IAttributeDictionary* GetAttributes() = 0;

    virtual i64 GetPriority() = 0;
    virtual void SetPriority(i64 value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemberClient)

IMemberClientPtr CreateMemberClient(
    TMemberClientConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    TString id,
    TString groupId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
