#pragma once

#include "public.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/ref_counted.h>

#include <yt/core/net/address.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public NConcurrency::IAsyncOutputStream
    , public NConcurrency::IAsyncInputStream
{
    virtual TFuture<void> WriteV(const TSharedRefArray& data) = 0;

    virtual TFuture<void> CloseRead() = 0;
    virtual TFuture<void> CloseWrite() = 0;

    virtual const TNetworkAddress& LocalAddress() const = 0;
    virtual const TNetworkAddress& RemoteAddress() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

std::pair<IConnectionPtr, IConnectionPtr> CreateConnectionPair(const NConcurrency::IPollerPtr& poller);

//! File descriptor must be in nonblocking mode.
IConnectionPtr CreateConnectionFromFD(
    int fd,
    const TNetworkAddress& localAddress,
    const TNetworkAddress& remoteAddress,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
