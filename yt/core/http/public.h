#pragma once

#include <yt/core/misc/common.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THeaders)

DECLARE_REFCOUNTED_STRUCT(IRequest)
DECLARE_REFCOUNTED_STRUCT(IResponse)
DECLARE_REFCOUNTED_STRUCT(IResponseWriter)

DECLARE_REFCOUNTED_STRUCT(IServer)
DECLARE_REFCOUNTED_STRUCT(IClient)

DECLARE_REFCOUNTED_STRUCT(IHttpHandler)

DECLARE_REFCOUNTED_CLASS(THttpIOConfig)
DECLARE_REFCOUNTED_CLASS(TServerConfig)
DECLARE_REFCOUNTED_CLASS(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

extern const TString DefaultServer;
extern const TString DefaultUserAgent;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
