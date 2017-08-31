#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>
#include <yt/core/misc/address.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

struct IDialer
    : public virtual TRefCounted
{
    virtual TFuture<IConnectionPtr> Dial(const TNetworkAddress& remote) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDialer);

IDialerPtr CreateDialer(const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
