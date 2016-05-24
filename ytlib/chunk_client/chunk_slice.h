#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_slice.pb.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/new.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern const int DefaultPartIndex;

////////////////////////////////////////////////////////////////////////////////

class TChunkSlice
    : public TIntrinsicRefCounted
{
    DECLARE_BYVAL_RW_PROPERTY(i64, DataSize);
    DECLARE_BYVAL_RW_PROPERTY(i64, RowCount);

    DECLARE_BYVAL_RO_PROPERTY(bool, SizeOverridden);
    DECLARE_BYVAL_RO_PROPERTY(int, PartIndex);
    DECLARE_BYVAL_RO_PROPERTY(i64, MaxBlockSize);

    DEFINE_BYVAL_RO_PROPERTY(TRefCountedChunkSpecPtr, ChunkSpec);
    DEFINE_BYREF_RO_PROPERTY(TReadLimit, LowerLimit);
    DEFINE_BYREF_RO_PROPERTY(TReadLimit, UpperLimit);

public:
    TChunkSlice() = default;
    TChunkSlice(TChunkSlice&& other) = default;

    TChunkSlice(
        TRefCountedChunkSpecPtr chunkSpec,
        const TNullable<NTableClient::TOwningKey>& lowerKey,
        const TNullable<NTableClient::TOwningKey>& upperKey);

    TChunkSlice(
        const TIntrusivePtr<const TChunkSlice>& chunkSlice,
        const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
        const TNullable<NTableClient::TOwningKey>& upperKey = Null);

    TChunkSlice(
        TRefCountedChunkSpecPtr chunkSpec,
        int partIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TChunkSlice(
        TRefCountedChunkSpecPtr chunkSpec,
        const NProto::TChunkSlice& protoChunkSlice);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    std::vector<TChunkSlicePtr> SliceEvenly(i64 sliceDataSize) const;

    i64 GetLocality(int replicaIndex) const;

    void SetKeys(const NTableClient::TOwningKey& lowerKey, const NTableClient::TOwningKey& upperKey);

    void Persist(NPhoenix::TPersistenceContext& context);

    friend size_t SpaceUsed(const TChunkSlicePtr chunkSlice);

private:
    int PartIndex_ = DefaultPartIndex;

    bool SizeOverridden_ = false;
    i64 RowCount_ = 0;
    i64 DataSize_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkSlice)

////////////////////////////////////////////////////////////////////////////////

//! Returns the size allocated for TChunkSlice.
//! This function is used for ref counted tracking.
size_t SpaceUsed(const TChunkSlicePtr p);

Stroka ToString(TChunkSlicePtr slice);

////////////////////////////////////////////////////////////////////////////////

//! Constructs a new chunk slice from the chunk spec, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
    const TNullable<NTableClient::TOwningKey>& upperKey = Null);

//! Constructs a new chunk slice from another slice, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TChunkSlicePtr CreateChunkSlice(
    TChunkSlicePtr other,
    const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
    const TNullable<NTableClient::TOwningKey>& upperKey = Null);

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const NProto::TChunkSlice& protoChunkSlice);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId);

std::vector<TChunkSlicePtr> SliceChunk(
    TRefCountedChunkSpecPtr chunkSpec,
    i64 sliceDataSize,
    int keyColumnCount,
    bool sliceByKeys);

std::vector<TChunkSlicePtr> SliceChunkByRowIndexes(TRefCountedChunkSpecPtr chunkSpec, i64 sliceDataSize);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlicePtr& chunk_slice);
void ToProto(NProto::TChunkSlice* protoChunkSlice, const TChunkSlicePtr& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

