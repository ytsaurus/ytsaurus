#pragma once

#include "public.h"

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(ui32* value, TChunkReplica replica);
void FromProto(TChunkReplica* replica, ui32 value);

////////////////////////////////////////////////////////////////////////////////

//! A compact representation of |(nodeId, index)| pair.
// TODO(babenko): rename since it now represents a replica of tablet as well
class TChunkReplica
{
public:
    TChunkReplica();
    TChunkReplica(int nodeId, int index);

    int GetNodeId() const;
    int GetIndex() const;

private:
    /*!
     *  Bits:
     *   0-27: node id
     *  28-31: index
     */
    ui32 Value;

    explicit TChunkReplica(ui32 value);

    friend void ToProto(ui32* value, TChunkReplica replica);
    friend void FromProto(TChunkReplica* replica, ui32 value);

};

Stroka ToString(TChunkReplica replica);

///////////////////////////////////////////////////////////////////////////////

struct TChunkIdWithIndex
{
    TChunkIdWithIndex();
    TChunkIdWithIndex(const TChunkId& id, int index);

    TChunkId Id;
    int Index;

};

///////////////////////////////////////////////////////////////////////////////

const int GenericChunkReplicaIndex = 0;    // no specific replica; the default one for regular chunks
const int AllChunkReplicasIndex    = 255;  // passed to various APIs to indicate that any replica is OK

// Journal chunks only:
const int ActiveChunkReplicaIndex   = 1; // the replica is currently being written
const int UnsealedChunkReplicaIndex = 2; // the replica is finished but not sealed yet
const int SealedChunkReplicaIndex   = 3; // the replica is finished and sealed

//! Valid indexes (excluding AllChunkReplicasIndex) are in range |[0, ChunkReplicaIndexBound)|.
const int ChunkReplicaIndexBound = 16;

//! For pretty-printing only.
DEFINE_ENUM(EJournalReplicaType,
    ((Generic)   (GenericChunkReplicaIndex))
    ((Active)    (ActiveChunkReplicaIndex))
    ((Unsealed)  (UnsealedChunkReplicaIndex))
    ((Sealed)    (SealedChunkReplicaIndex))
);

///////////////////////////////////////////////////////////////////////////////

bool operator == (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);
bool operator != (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs);

Stroka ToString(const TChunkIdWithIndex& id);

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

    Stroka operator() (TChunkReplica replica) const;

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

DECLARE_PODTYPE(NYT::NChunkClient::TChunkIdWithIndex)

//! A hasher for TChunkIdWithIndex.
template <>
struct hash<NYT::NChunkClient::TChunkIdWithIndex>
{
    inline size_t operator()(const NYT::NChunkClient::TChunkIdWithIndex& value) const
    {
        return THash<NYT::NChunkClient::TChunkId>()(value.Id) * 497 +
            value.Index;
    }
};

///////////////////////////////////////////////////////////////////////////////
