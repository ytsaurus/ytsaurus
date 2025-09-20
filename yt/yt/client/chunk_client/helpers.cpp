#include "helpers.h"

#include "read_limit.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

using NYT::FromProto;
using NYT::ToProto;

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TReadRange& readRange, std::ostream* os)
{
    *os << ToString(readRange);
}

NObjectClient::TObjectId GetObjectIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    return FromProto<NObjectClient::TObjectId>(chunkSpec.chunk_id());
}

NObjectClient::TCellId GetCellIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    return FromProto<NObjectClient::TCellId>(chunkSpec.cell_id());
}

NObjectClient::TObjectId GetTabletIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    return FromProto<NTabletClient::TTabletId>(chunkSpec.tablet_id());
}

TChunkReplicaWithMediumList GetReplicasFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    if (chunkSpec.replica_specs_size() != 0) {
        return FromProto<TChunkReplicaWithMediumList>(chunkSpec.replica_specs());
    } else if (chunkSpec.replicas_size() != 0) {
        return FromProto<TChunkReplicaWithMediumList>(chunkSpec.replicas());
    } else {
        auto legacyReplicas = FromProto<TChunkReplicaList>(chunkSpec.legacy_replicas());
        TChunkReplicaWithMediumList replicas;
        replicas.reserve(legacyReplicas.size());
        for (auto legacyReplica : legacyReplicas) {
            replicas.emplace_back(legacyReplica);
        }
        return replicas;
    }
}

TReplicasByType GetReplicasByType(const TChunkReplicaWithMediumList& replicas)
{
    TReplicasByType result;

    for (const auto& replica : replicas) {
        if (replica.GetNodeId() == OffshoreNodeId) {
            result.OffshoreReplicas.push_back(replica);
        } else {
            result.DomesticReplicas.push_back(replica);
        }
    }

    return result;
}

void VerifyNoOffshoreReplicas(const TChunkReplicaWithMediumList& replicas)
{
    auto replicasByType = GetReplicasByType(replicas);
    YT_VERIFY(replicasByType.OffshoreReplicas.empty());
}

void SetTabletId(NProto::TChunkSpec* chunkSpec, NTabletClient::TTabletId tabletId)
{
    ToProto(chunkSpec->mutable_tablet_id(), tabletId);
}

void SetObjectId(NProto::TChunkSpec* chunkSpec, NObjectClient::TObjectId objectId)
{
    ToProto(chunkSpec->mutable_chunk_id(), objectId);
}

////////////////////////////////////////////////////////////////////////////////

EExternalSourceFormat DeduceExternalSourceFormatOrThrow(TStringBuf fileName)
{
    if (fileName.ends_with(".json") || fileName.ends_with(".jsonl")) {
        return EExternalSourceFormat::Jsonl;
    } else if (fileName.ends_with(".parquet")) {
        return EExternalSourceFormat::Parquet;
    } else if (fileName.ends_with(".csv")) {
        return EExternalSourceFormat::Csv;
    } else {
        THROW_ERROR_EXCEPTION("Cannot deduce external source format from file name %Qv; only .parquet, .json, .jsonl and .csv extensions are supported",
            fileName);
    }
}

EChunkFormat GetChunkFormatFromExternalSourceFormat(EExternalSourceFormat externalFormat)
{
    switch (externalFormat) {
        case EExternalSourceFormat::Parquet:
            return EChunkFormat::TableUnversionedArrowParquet;
        case EExternalSourceFormat::Jsonl:
            return EChunkFormat::TableUnversionedArrowJsonLines;
        case EExternalSourceFormat::Csv:
            return EChunkFormat::TableUnversionedArrowCsv;
        default:
            THROW_ERROR_EXCEPTION("Unexpected external source format %Qlv",
                externalFormat);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
