#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/phoenix.h>

#include <ytlib/chunk_client/chunk_slice.pb.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSlice
    : public TIntrinsicRefCounted
{
public:
    //! Use #CreateChunkSlice instead.
    TChunkSlice();

    TChunkSlice(const TChunkSlice& other);

    TChunkSlice(TChunkSlice&& other);

    TChunkSlice(
        TRefCountedChunkSpecPtr chunkSpec,
        const TNullable<NTableClient::TOwningKey>& lowerKey,
        const TNullable<NTableClient::TOwningKey>& upperKey);

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

    TRefCountedChunkSpecPtr ChunkSpec() const;
    int GetPartIndex() const;
    const TReadLimit& LowerLimit() const;
    const TReadLimit& UpperLimit() const;
    const NProto::TSizeOverrideExt& SizeOverrideExt() const;

    i64 GetMaxBlockSize() const;
    i64 GetDataSize() const;
    i64 GetRowCount() const;

    void SetDataSize(i64 data);
    void SetRowCount(i64 rowCount);
    void SetLowerRowIndex(i64 rowIndex);
    void SetKeys(const NTableClient::TOwningKey& lowerKey, const NTableClient::TOwningKey& upperKey);

    void Persist(NPhoenix::TPersistenceContext& context);

private:
    TRefCountedChunkSpecPtr ChunkSpec_;
    int PartIndex_ = -1;

    TReadLimit LowerLimit_;
    TReadLimit UpperLimit_;
    NProto::TSizeOverrideExt SizeOverrideExt_;
};

DEFINE_REFCOUNTED_TYPE(TChunkSlice)

////////////////////////////////////////////////////////////////////////////////

//! Constructs a new chunk slice from the original one, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
    const TNullable<NTableClient::TOwningKey>& upperKey = Null);

//! Constructs a new chunk slice from the original one, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const NProto::TChunkSlice& protoChunkSlice);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId);

std::vector<TChunkSlicePtr> SliceChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec,
    i64 sliceDataSize,
    int keyColumnCount);

std::vector<TChunkSlicePtr> SliceChunkByRowIndexes(TRefCountedChunkSpecPtr chunkSpec, i64 sliceDataSize);

void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice);
void ToProto(NProto::TChunkSlice* protoChunkSlice, const TChunkSlice& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

