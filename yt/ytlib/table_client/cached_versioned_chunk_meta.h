#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/public.h>

#include <yt/core/actions/future.h>

#include <memory>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TIntrinsicRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(TOwningKey, MinKey);
    DEFINE_BYREF_RO_PROPERTY(TOwningKey, MaxKey);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TOwningKey>, BlockLastKeys);
    DEFINE_BYREF_RO_PROPERTY(NProto::TBlockMetaExt, BlockMeta);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RO_PROPERTY(TTableSchema, ChunkSchema);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TMiscExt, Misc);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TColumnIdMapping>, SchemaIdMapping);
    DEFINE_BYVAL_RO_PROPERTY(int, ChunkKeyColumnCount);
    DEFINE_BYVAL_RO_PROPERTY(int, KeyColumnCount);

    static TFuture<TCachedVersionedChunkMetaPtr> Load(
        NChunkClient::IChunkReaderPtr chunkReader,
        const TTableSchema& schema);

private:
    TCachedVersionedChunkMetaPtr DoLoad(
        NChunkClient::IChunkReaderPtr chunkReader,
        const TTableSchema& readerSchema);

    void ValidateChunkMeta();
    void ValidateSchema(const TTableSchema& readerSchema);
};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
