#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/attributes.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/rpc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TMemberClient
    : public TRefCounted
{
public:
    TMemberClient(
        TMemberClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        TString id,
        TString groupId);
    ~TMemberClient();

    void Start();
    void Stop();

    NYTree::IAttributeDictionary* GetAttributes();

    i64 GetPriority();
    void SetPriority(i64 value);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TMemberClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
