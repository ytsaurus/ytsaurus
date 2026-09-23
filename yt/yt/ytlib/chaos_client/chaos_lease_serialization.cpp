#include "chaos_lease_serialization.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void FromProto(TChaosLease* chaosLease, const NProto::TRspGetChaosLease& protoResponse)
{
    chaosLease->Timeout = NYT::FromProto<TDuration>(protoResponse.timeout());
    chaosLease->LastPingTime = NYT::FromProto<TInstant>(protoResponse.last_ping_time());
    NYT::FromProto(&chaosLease->CoordinatorCellIds, protoResponse.coordinator_cell_ids());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
