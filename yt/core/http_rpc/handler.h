#pragma once

#include <yt/core/http/public.h>
#include <yt/core/rpc/public.h>

namespace NYT {
namespace NHttpRpc {

////////////////////////////////////////////////////////////////////////////////

NYT::NHttp::IHttpHandlerPtr CreateRpcHttpHandler(NYT::NRpc::IServicePtr service, const TString& baseUrl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpRpc
} // namespace NYT
