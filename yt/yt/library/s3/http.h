#pragma once

#include "credential_provider.h"
#include "public.h"

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/http/http.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

struct THttpRequest final
{
    //! Method (GET, POST, PUT, etc).
    NHttp::EMethod Method;

    //! Protocol (usually http).
    std::string Protocol;

    //! Host and port.
    std::string Host;
    std::optional<ui16> Port;

    //! Request path.
    std::string Path;

    //! Region (S3 specific).
    std::string Region;

    // Service (S3 specific).
    std::string Service;

    //! Request query (for example k=v for ytsaurus.tech/docs?k=v).
    THashMap<std::string, std::string> Query;

    //! Request headers.
    THashMap<std::string, std::string> Headers;

    //! Payload (null if no payload).
    TSharedRef Payload;
};

////////////////////////////////////////////////////////////////////////////////

//! Prepares HTTP request for sending. Time is used for unittests only.
void PrepareHttpRequest(
    THttpRequest* request,
    ICredentialsProviderPtr credentialProvider,
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
    bool useTls,
    const NCrypto::TSslContextConfigPtr& sslContextConfig,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
