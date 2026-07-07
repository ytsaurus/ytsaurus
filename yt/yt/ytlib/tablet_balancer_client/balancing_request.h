#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTabletBalancerClient {

////////////////////////////////////////////////////////////////////////////////

struct TBalancingRequest
{
    std::string BundleName;
    std::vector<NTabletClient::TTabletId> TabletIds;
    EBalancingRequestMode Mode;
    std::optional<std::string> Reason;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TReqRequestBalancing* protoRequest,
    const TBalancingRequest& request);

void FromProto(
    TBalancingRequest* request,
    const NProto::TReqRequestBalancing& protoRequest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancerClient
