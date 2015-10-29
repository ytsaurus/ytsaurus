#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IRoamingChannelProvider
    : public virtual TRefCounted
{
    //! Cf. IChannel::GetEndpointTextDescription.
    virtual Stroka GetEndpointTextDescription() const = 0;

    //! Cf. IChannel::GetEndpointYsonDescription.
    virtual NYTree::TYsonString GetEndpointYsonDescription() const = 0;

    //! Returns the actual channel to be used for sending to service with
    //! a given #serviceName.
    virtual TFuture<IChannelPtr> GetChannel(const Stroka& serviceName) = 0;

    //! Terminates the cached channels, if any.
    virtual TFuture<void> Terminate(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRoamingChannelProvider)

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel with a dynamically discovered endpoint.
/*!
 *  The IRoamingChannelProvider::GetChannel is invoked to discover the actual endpoint.
 *  The channels are not reused between requests so it's provider's
 *  responsibility to provide the appropriate caching.
 */
IChannelPtr CreateRoamingChannel(IRoamingChannelProviderPtr provider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
