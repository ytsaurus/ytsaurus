#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

#include <ytlib/ytree/attributes.h>

#include <ytlib/chunk_client/input_chunk.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/erasure/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedInputChunk
    : public TIntrinsicRefCounted
    , public NProto::TInputChunk
{
    explicit TRefCountedInputChunk(const NProto::TInputChunk& other);
    explicit TRefCountedInputChunk(NProto::TInputChunk&& other);

    TRefCountedInputChunk(const TRefCountedInputChunk& other);
};

////////////////////////////////////////////////////////////////////////////////

class TInputChunkSlice
    : public TIntrinsicRefCounted
{
public:
    //! Use #CreateChunkSlice instead.
    TInputChunkSlice();

    TInputChunkSlice(const TInputChunkSlice& other);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    std::vector<TInputChunkSlicePtr> SliceEvenly(i64 sliceDataSize) const;

    i64 GetLocality(int replicaIndex) const;

    TRefCountedInputChunkPtr GetInputChunk() const;
    i64 GetDataSize() const;
    i64 GetRowCount() const;

private:
    TRefCountedInputChunkPtr InputChunk;
    int PartIndex;

    NProto::TReadLimit StartLimit;
    NProto::TReadLimit EndLimit;
    NProto::TSizeOverrideExt SizeOverrideExt;

    friend void ToProto(NProto::TInputChunk* inputChunk, const TInputChunkSlice& chunkSlice);

    friend TInputChunkSlicePtr CreateChunkSlice(
        TRefCountedInputChunkPtr inputChunk,
        const TNullable<NProto::TKey>& startKey,
        const TNullable<NProto::TKey>& endKey);

    friend void AppendErasureChunkSlices(
        TRefCountedInputChunkPtr inputChunk,
        NErasure::ECodec codecId,
        std::vector<TInputChunkSlicePtr>* slices);

};

//! Constructs a new chunk slice from the original chunk one and restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputChunkSlicePtr CreateChunkSlice(
    TRefCountedInputChunkPtr inputChunk,
    const TNullable<NProto::TKey>& startKey = Null,
    const TNullable<NProto::TKey>& endKey = Null);

//! Constructs separate chunk slice for each part of erasure chunk. Pushes
void AppendErasureChunkSlices(
    TRefCountedInputChunkPtr inputChunk,
    NErasure::ECodec codecId,
    std::vector<TInputChunkSlicePtr>* slices);

void ToProto(NProto::TInputChunk* inputChunk, const TInputChunkSlice& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

bool IsNontrivial(const NProto::TReadLimit& limit);

//! Extracts various chunk statistics by first looking at
//! TSizeOverrideExt (if present) and then at TMiscExt.
void GetStatistics(
    const NProto::TInputChunk& inputChunk,
    i64* dataSize = nullptr,
    i64* rowCount = nullptr,
    i64* valueCount = nullptr);

//! Construcs a new chunk slice removing any limits from origin.
TRefCountedInputChunkPtr CreateCompleteChunk(TRefCountedInputChunkPtr inputChunk);

TChunkId EncodeChunkId(
    const NProto::TInputChunk& inputChunk,
    NNodeTrackerClient::TNodeId nodeId);

////////////////////////////////////////////////////////////////////////////////

bool ExtractOverwriteFlag(const NYTree::IAttributeDictionary& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

