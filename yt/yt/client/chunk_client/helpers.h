#pragma once

#include "chunk_replica.h"

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TReadRange& readRange, std::ostream* os);

NObjectClient::TObjectId GetObjectIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec);
NObjectClient::TCellId GetCellIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec);
NTabletClient::TTabletId GetTabletIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec);
TChunkReplicaWithMediumList GetReplicasFromChunkSpec(const NProto::TChunkSpec& chunkSpec);

struct TReplicasByType
{
    TChunkReplicaWithMediumList DomesticReplicas;
    TChunkReplicaWithMediumList OffshoreReplicas;
};
TReplicasByType GetReplicasByType(const TChunkReplicaWithMediumList& replicas);
void VerifyNoOffshoreReplicas(const TChunkReplicaWithMediumList& replicas);

void SetTabletId(NProto::TChunkSpec* chunkSpec, NTabletClient::TTabletId tabletId);
void SetObjectId(NProto::TChunkSpec* chunkSpec, NObjectClient::TObjectId objectId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
