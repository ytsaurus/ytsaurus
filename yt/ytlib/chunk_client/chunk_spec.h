#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/phoenix.h>

#include <core/ytree/attributes.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <core/erasure/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedChunkSpec
    : public TIntrinsicRefCounted
    , public NProto::TChunkSpec
{
    TRefCountedChunkSpec();
    explicit TRefCountedChunkSpec(const NProto::TChunkSpec& other);
    explicit TRefCountedChunkSpec(NProto::TChunkSpec&& other);
    TRefCountedChunkSpec(const TRefCountedChunkSpec& other);

};

////////////////////////////////////////////////////////////////////////////////

class TChunkSlice
    : public TIntrinsicRefCounted
{
public:
    //! Use #CreateChunkSlice instead.
    TChunkSlice();

    TChunkSlice(const TChunkSlice& other);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    std::vector<TChunkSlicePtr> SliceEvenly(i64 sliceDataSize) const;

    i64 GetLocality(int replicaIndex) const;

    TRefCountedChunkSpecPtr GetChunkSpec() const;
    i64 GetDataSize() const;
    i64 GetRowCount() const;

    i64 GetMaxBlockSize() const;

    void Persist(NPhoenix::TPersistenceContext& context);

private:
    TRefCountedChunkSpecPtr ChunkSpec;
    int PartIndex;

    NProto::TReadLimit StartLimit;
    NProto::TReadLimit EndLimit;
    NProto::TSizeOverrideExt SizeOverrideExt;

    friend void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice);

    friend TChunkSlicePtr CreateChunkSlice(
        TRefCountedChunkSpecPtr chunkSpec,
        const TNullable<NProto::TKey>& startKey,
        const TNullable<NProto::TKey>& endKey);

    friend std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
        TRefCountedChunkSpecPtr chunkSpec,
        NErasure::ECodec codecId);

};

//! Constructs a new chunk slice from the original one, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NProto::TKey>& startKey = Null,
    const TNullable<NProto::TKey>& endKey = Null);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId);

void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice);

bool IsNontrivial(const NProto::TReadLimit& limit);
bool IsTrivial(const NProto::TReadLimit& limit);

bool IsUnavailable(
    const NProto::TChunkSpec& chunkSpec,
    bool checkParityParts = false);
bool IsUnavailable(
    const TChunkReplicaList& replicas,
    NErasure::ECodec codecId,
    bool checkParityParts = false);

//! Extracts various chunk statistics by first looking at
//! TSizeOverrideExt (if present) and then at TMiscExt.
void GetStatistics(
    const NProto::TChunkSpec& chunkSpec,
    i64* dataSize = nullptr,
    i64* rowCount = nullptr,
    i64* valueCount = nullptr);

//! Constructs a new chunk slice removing any limits from origin.
TRefCountedChunkSpecPtr CreateCompleteChunk(TRefCountedChunkSpecPtr chunkSpec);

TChunkId EncodeChunkId(
    const NProto::TChunkSpec& chunkSpec,
    NNodeTrackerClient::TNodeId nodeId);

bool ExtractOverwriteFlag(const NYTree::IAttributeDictionary& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

