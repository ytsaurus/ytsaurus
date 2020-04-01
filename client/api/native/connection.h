#pragma once

#include "public.h"

#include <yt/core/rpc/client.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public virtual NYT::TRefCounted
{
    virtual NYT::NRpc::IChannelPtr GetChannel() = 0;

    virtual NYT::TErrorOr<NYT::NRpc::IChannelPtr> GetChannel(int instanceTag) = 0;

    virtual void SetupRequestAuthentication(
        const TIntrusivePtr<NYT::NRpc::TClientRequest>& request) = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(TConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
