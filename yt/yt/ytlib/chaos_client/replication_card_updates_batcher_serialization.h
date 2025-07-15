#pragma once

#include "replication_card_updates_batcher.h"

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TReplicationCardProgressUpdate* replicationCardProgressUpdate,
    const NChaosClient::NProto::TReplicationCardProgressUpdateItem& protoReplicationCardProgressUpdate);

void ToProto(
    NChaosClient::NProto::TReplicationCardProgressUpdateItem* protoReplicationCardProgressUpdate,
    const TReplicationCardProgressUpdate& replicationCardProgressUpdate);

void FromProto(
    TReplicationCardProgressUpdatesBatch* batch,
    const NChaosClient::NProto::TReqUpdateMultipleTableProgresses& protoBatch);

// Nullable.
TReplicationCardPtr FromProto(const NChaosClient::NProto::TRspUpdateTableProgress& protoResponse);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
