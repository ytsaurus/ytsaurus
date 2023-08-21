#pragma once

#include "public.h"

#include <yt/yt/core/http/http.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

struct THttpRequest final
{
    //! Method (GET, POST, PUT, etc).
    NHttp::EMethod Method;

    //! Protocol (usually http).
    TString Protocol;

    //! Host and port.
    TString Host;
    std::optional<ui16> Port;

    //! Request path.
    TString Path;

    //! Region (S3 specific).
    TString Region;

    // Service (S3 specific).
    TString Service;

    //! Request query (for example k=v for ytsaurus.tech/docs?k=v).
    THashMap<TString, TString> Query;

    //! Request headers.
    THashMap<TString, TString> Headers;

    //! Payload (null if no payload).
    TSharedRef Payload;
};

////////////////////////////////////////////////////////////////////////////////

//! Prepares HTTP request for sending. Time is used for unittests only.
void PrepareHttpRequest(
    THttpRequest* request,
    const TString& keyId,
    const TString& secretKey,
    TInstant requestTime = TInstant::Now());

NHttp::IResponsePtr MakeRequest(
    const THttpRequest& request,
    IInvokerPtr responseInvoker);

////////////////////////////////////////////////////////////////////////////////

struct IHttpClient
    : public TRefCounted
{
    virtual TFuture<void> Start() = 0;

    virtual TFuture<NHttp::IResponsePtr> MakeRequest(THttpRequest request) = 0;
};

DECLARE_REFCOUNTED_STRUCT(IHttpClient)
DEFINE_REFCOUNTED_TYPE(IHttpClient)

////////////////////////////////////////////////////////////////////////////////

IHttpClientPtr CreateHttpClient(
    NHttp::TClientConfigPtr config,
    NNet::TNetworkAddress address,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
