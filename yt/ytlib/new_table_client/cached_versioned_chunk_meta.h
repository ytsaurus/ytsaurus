#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "schema.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/misc/property.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TRefCounted
{
    DEFINE_BYREF_RO_PROPERTY(NProto::TBlockIndexExt, BlockIndex);
    DEFINE_BYREF_RO_PROPERTY(NProto::TBlockMetaExt, BlockMeta);
    DEFINE_BYREF_RO_PROPERTY(NProto::TBoundaryKeysExt, BoundaryKeys);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RO_PROPERTY(TTableSchema, ChunkSchema);
    DEFINE_BYREF_RO_PROPERTY(TKeyColumns, KeyColumns);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TMiscExt, Misc);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TColumnIdMapping>, SchemaIdMapping);

public:
    TCachedVersionedChunkMeta(
        NChunkClient::IAsyncReaderPtr asyncReader,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns);

    TAsyncError Load();

private:
    NChunkClient::IAsyncReaderPtr AsyncReader_;
    const TTableSchema ReaderSchema_;

    TError DoLoad();
    void ReleaseReader(TError error);
    TError ValidateSchema();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
