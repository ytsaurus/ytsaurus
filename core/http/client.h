#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/net/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public virtual TRefCounted
{
    virtual TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const NNet::IDialerPtr& dialer,
    const IInvokerPtr& invoker);
IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
