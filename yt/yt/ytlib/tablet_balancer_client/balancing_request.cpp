#include "balancing_request.h"

#include <yt/yt/ytlib/tablet_balancer_client/proto/tablet_balancer_service.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTabletBalancerClient {

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TReqRequestBalancing* protoRequest,
    const TBalancingRequest& request)
{
    ToProto(protoRequest->mutable_bundle_name(), request.BundleName);
    ToProto(protoRequest->mutable_tablet_ids(), request.TabletIds);
    protoRequest->set_mode(ToProto(request.Mode));
    YT_OPTIONAL_TO_PROTO(protoRequest, reason, request.Reason);
}

void FromProto(
    TBalancingRequest* request,
    const NProto::TReqRequestBalancing& protoRequest)
{
    FromProto(&request->BundleName, protoRequest.bundle_name());
    FromProto(&request->TabletIds, protoRequest.tablet_ids());
    FromProto(&request->Mode, protoRequest.mode());
    request->Reason = YT_OPTIONAL_FROM_PROTO(protoRequest, reason);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancerClient
