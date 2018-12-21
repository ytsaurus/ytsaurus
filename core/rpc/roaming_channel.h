#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/error.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IRoamingChannelProvider
    : public virtual TRefCounted
{
    //! Cf. IChannel::GetEndpointDescription.
    virtual const TString& GetEndpointDescription() const = 0;

    //! Cf. IChannel::GetEndpointAttributes.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    //! Returns the actual channel to be used for sending to service with
    //! a given #serviceName.
    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& request) = 0;

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

} // namespace NYT::NRpc
