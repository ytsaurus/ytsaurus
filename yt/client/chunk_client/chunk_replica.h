#pragma once

#include "public.h"

#include <yt/client/node_tracker_client/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(ui32* value, TChunkReplica replica);
void FromProto(TChunkReplica* replica, ui32 value);

////////////////////////////////////////////////////////////////////////////////

//! A compact representation of |(nodeId, replicaIndex, mediumIndex)| triplet.
class TChunkReplica
{
public:
    TChunkReplica();
    TChunkReplica(int nodeId, int replicaIndex, int mediumIndex);

    int GetNodeId() const;
    int GetReplicaIndex() const;
    int GetMediumIndex() const;

private:
    /*!
     *  Bits:
     *   0-23: node id
     *  24-28: replica index (5 bits)
     *  29-31: medium index (3 bits)
     */
    ui32 Value;

    explicit TChunkReplica(ui32 value);

    friend void ToProto(ui32* value, TChunkReplica replica);
    friend void FromProto(TChunkReplica* replica, ui32 value);

};

TString ToString(TChunkReplica replica);

////////////////////////////////////////////////////////////////////////////////

struct TChunkIdWithIndex
{
    TChunkIdWithIndex();
    TChunkIdWithIndex(const TChunkId& id, int replicaIndex);

    TChunkId Id;
    int ReplicaIndex;
};

struct TChunkIdWithIndexes
    : public TChunkIdWithIndex
{
    TChunkIdWithIndexes();
    TChunkIdWithIndexes(const TChunkIdWithIndex& chunkIdWithIndex, int mediumIndex);
    TChunkIdWithIndexes(const TChunkId& id, int replicaIndex, int mediumIndex);

    int MediumIndex;
};

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);
bool operator!=(const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);

TString ToString(const TChunkIdWithIndex& id);

bool operator==(const TChunkIdWithIndexes& lhs, const TChunkIdWithIndexes& rhs);
bool operator!=(const TChunkIdWithIndexes& lhs, const TChunkIdWithIndexes& rhs);

TString ToString(const TChunkIdWithIndexes& id);

//! Returns |true| iff this is an artifact chunk.
bool IsArtifactChunkId(const TChunkId& id);

//! Returns |true| iff this is a journal chunk.
bool IsJournalChunkId(const TChunkId& id);

//! Returns |true| iff this is a erasure chunk.
bool IsErasureChunkId(const TChunkId& id);

//! Returns |true| iff this is a erasure chunk part.
bool IsErasureChunkPartId(const TChunkId& id);

//! Returns id for a part of a given erasure chunk.
TChunkId ErasurePartIdFromChunkId(const TChunkId& id, int index);

//! Returns the whole chunk id for a given erasure chunk part id.
TChunkId ErasureChunkIdFromPartId(const TChunkId& id);

//! Returns part index for a given erasure chunk part id.
int IndexFromErasurePartId(const TChunkId& id);

//! For usual chunks, preserves the id.
//! For erasure chunks, constructs the part id using the given replica index.
TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex);

//! For regular chunks, preserves the id and returns #GenericChunkReplicaIndex.
//! For erasure chunk parts, constructs the whole chunk id and extracts part index.
TChunkIdWithIndex DecodeChunkId(const TChunkId& id);

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaAddressFormatter
{
public:
    explicit TChunkReplicaAddressFormatter(NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

    void operator()(TStringBuilder* builder, TChunkReplica replica) const;

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

Y_DECLARE_PODTYPE(NYT::NChunkClient::TChunkIdWithIndex);

//! A hasher for TChunkIdWithIndex.
template <>
struct THash<NYT::NChunkClient::TChunkIdWithIndex>
{
    inline size_t operator()(const NYT::NChunkClient::TChunkIdWithIndex& value) const
    {
        return THash<NYT::NChunkClient::TChunkId>()(value.Id) * 497 + value.ReplicaIndex;
    }
};

Y_DECLARE_PODTYPE(NYT::NChunkClient::TChunkIdWithIndexes);

//! A hasher for TChunkIdWithIndexes.
template <>
struct THash<NYT::NChunkClient::TChunkIdWithIndexes>
{
    inline size_t operator()(const NYT::NChunkClient::TChunkIdWithIndexes& value) const
    {
        return THash<NYT::NChunkClient::TChunkId>()(value.Id) * 497 +
            value.ReplicaIndex + value.MediumIndex * 8;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_
