#pragma once

#include "public.h"

#include <yt/yt/ytlib/bundle_controller/proto/bundle_controller_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NBundleController {

////////////////////////////////////////////////////////////////////////////////

class TBundleControllerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TBundleControllerServiceProxy, BundleControllerService,
        .SetProtocolVersion(4));

    DEFINE_RPC_PROXY_METHOD(NProto, GetBundleConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
