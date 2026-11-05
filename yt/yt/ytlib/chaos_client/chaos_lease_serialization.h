#pragma once

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/client/chaos_client/chaos_lease.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void FromProto(TChaosLease* chaosLease, const NProto::TRspGetChaosLease& protoResponse);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
