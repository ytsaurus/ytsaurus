#include "chunk_replica.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/library/erasure/public.h>

#include <yt/core/misc/format.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TChunkReplicaWithMedium replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.GetNodeId());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    if (replica.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.GetMediumIndex());
    }
}

TString ToString(TChunkReplicaWithMedium replica)
{
    return ToStringViaBuilder(replica);
}

void FormatValue(TStringBuilderBase* builder, TChunkReplica replica, TStringBuf spec)
{
    builder->AppendFormat("%v", replica.GetNodeId());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
}

TString ToString(TChunkReplica replica)
{
    return ToStringViaBuilder(replica);
}

void FormatValue(TStringBuilderBase* builder, const TChunkIdWithIndex& id, TStringBuf spec)
{
    builder->AppendFormat("%v", id.Id);
    if (id.ReplicaIndex != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", id.ReplicaIndex);
    }
}

TString ToString(const TChunkIdWithIndex& id)
{
    return ToStringViaBuilder(id);
}

void FormatValue(TStringBuilderBase* builder, const TChunkIdWithIndexes& id, TStringBuf spec)
{
    builder->AppendFormat("%v", id.Id);
    if (id.ReplicaIndex != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", id.ReplicaIndex);
    }
    if (id.MediumIndex == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (id.MediumIndex != GenericMediumIndex) {
        builder->AppendFormat("@%v", id.MediumIndex);
    }
}

TString ToString(const TChunkIdWithIndexes& id)
{
    return ToStringViaBuilder(id);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicaAddressFormatter::TChunkReplicaAddressFormatter(TNodeDirectoryPtr nodeDirectory)
    : NodeDirectory_(std::move(nodeDirectory))
{ }

void TChunkReplicaAddressFormatter::operator()(TStringBuilderBase* builder, TChunkReplicaWithMedium replica) const
{
    const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
    if (descriptor) {
        builder->AppendFormat("%v", descriptor->GetDefaultAddress());
    } else {
        builder->AppendFormat("<unresolved-%v>", replica.GetNodeId());
    }
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    if (replica.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.GetMediumIndex());
    }
}

void TChunkReplicaAddressFormatter::operator()(TStringBuilderBase* builder, TChunkReplica replica) const
{
    const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
    if (descriptor) {
        builder->AppendFormat("%v", descriptor->GetDefaultAddress());
    } else {
        builder->AppendFormat("<unresolved-%v>", replica.GetNodeId());
    }
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

EObjectType BaseErasurePartTypeFromPartId(TChunkId id)
{
    auto type = TypeFromId(id);
    if (type >= MinErasureChunkPartType && type <= MaxErasureChunkPartType) {
        return EObjectType::ErasureChunkPart_0;
    } else if (type >= MinErasureJournalChunkPartType && type <= MaxErasureJournalChunkPartType) {
        return EObjectType::ErasureJournalChunkPart_0;
    } else {
        YT_ABORT();
    }
}

EObjectType BaseErasurePartTypeFromWholeId(TChunkId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::ErasureChunk:        return EObjectType::ErasureChunkPart_0;
        case EObjectType::ErasureJournalChunk: return EObjectType::ErasureJournalChunkPart_0;
        default:                               YT_ABORT();
    }
}

EObjectType WholeErasureTypeFromPartId(TChunkId id)
{
    auto type = TypeFromId(id);
    if (type >= MinErasureChunkPartType && type <= MaxErasureChunkPartType) {
        return EObjectType::ErasureChunk;
    } else if (type >= MinErasureJournalChunkPartType && type <= MaxErasureJournalChunkPartType) {
        return EObjectType::ErasureJournalChunk;
    } else {
        YT_ABORT();
    }
}

} // namespace

TChunkId ErasurePartIdFromChunkId(TChunkId id, int index)
{
    return ReplaceTypeInId(id, static_cast<EObjectType>(static_cast<int>(BaseErasurePartTypeFromWholeId(id)) + index));
}

TChunkId ErasureChunkIdFromPartId(TChunkId id)
{
    return ReplaceTypeInId(id, WholeErasureTypeFromPartId(id));
}

int ReplicaIndexFromErasurePartId(TChunkId id)
{
    int index = static_cast<int>(TypeFromId(id)) - static_cast<int>(BaseErasurePartTypeFromPartId(id));
    YT_VERIFY(index >= 0 && index < ChunkReplicaIndexBound);
    return index;
}

TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex)
{
    return IsErasureChunkId(idWithIndex.Id)
        ? ErasurePartIdFromChunkId(idWithIndex.Id, idWithIndex.ReplicaIndex)
        : idWithIndex.Id;
}

TChunkIdWithIndex DecodeChunkId(TChunkId id)
{
    return IsErasureChunkPartId(id)
        ? TChunkIdWithIndex(ErasureChunkIdFromPartId(id), ReplicaIndexFromErasurePartId(id))
        : TChunkIdWithIndex(id, GenericChunkReplicaIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
