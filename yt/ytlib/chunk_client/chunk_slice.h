#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/phoenix.h>

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/new_table_client/unversioned_row.h>

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

    ~TChunkSlice();

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

    TReadLimit LowerLimit;
    TReadLimit UpperLimit;
    NProto::TSizeOverrideExt SizeOverrideExt;

    friend void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice);

    friend TChunkSlicePtr CreateChunkSlice(
        TRefCountedChunkSpecPtr chunkSpec,
        const TNullable<NVersionedTableClient::TOwningKey>& lowerKey,
        const TNullable<NVersionedTableClient::TOwningKey>& upperKey);

    // XXX(sandello): Do we really need codecId here?
    friend std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
        TRefCountedChunkSpecPtr chunkSpec,
        NErasure::ECodec codecId);

};

DEFINE_REFCOUNTED_TYPE(TChunkSlice)

////////////////////////////////////////////////////////////////////////////////

//! Constructs a new chunk slice from the original one, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NVersionedTableClient::TOwningKey>& startKey = Null,
    const TNullable<NVersionedTableClient::TOwningKey>& endKey = Null);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId);

void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

