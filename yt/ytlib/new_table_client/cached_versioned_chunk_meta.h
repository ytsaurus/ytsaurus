#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/misc/property.h>
#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TIntrinsicRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, MinKey);
    DEFINE_BYVAL_RO_PROPERTY(TOwningKey, MaxKey);
    DEFINE_BYREF_RO_PROPERTY(NProto::TBlockIndexExt, BlockIndex);
    DEFINE_BYREF_RO_PROPERTY(NProto::TBlockMetaExt, BlockMeta);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RO_PROPERTY(TTableSchema, ChunkSchema);
    DEFINE_BYREF_RO_PROPERTY(TKeyColumns, KeyColumns);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TMiscExt, Misc);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TColumnIdMapping>, SchemaIdMapping);

    static TFuture<TErrorOr<TCachedVersionedChunkMetaPtr>> Load(
        NChunkClient::IReaderPtr asyncReader,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns);

private:
    TErrorOr<TCachedVersionedChunkMetaPtr> DoLoad(
        NChunkClient::IReaderPtr chunkReader,
        const TTableSchema& readerSchema,
        const TKeyColumns& keyColumns);

    void ValidateChunkMeta();
    void ValidateSchema(const TTableSchema& readerSchema);

};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
