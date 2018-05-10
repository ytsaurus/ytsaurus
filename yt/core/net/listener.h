#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>

#include <yt/core/net/address.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

struct IListener
    : public virtual TRefCounted
{
    virtual const TNetworkAddress& GetAddress() const = 0;

    virtual TFuture<IConnectionPtr> Accept() = 0;
};

DEFINE_REFCOUNTED_TYPE(IListener);

IListenerPtr CreateListener(
    const TNetworkAddress& address,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
