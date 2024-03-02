#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "hunks.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnarChunkMeta
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::EChunkType, ChunkType);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::EChunkFormat, ChunkFormat);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::EChunkFeatures, ChunkFeatures);
    DEFINE_BYREF_RO_PROPERTY(TRefCountedDataBlockMetaPtr, DataBlockMeta);
    DEFINE_BYREF_RO_PROPERTY(TRefCountedColumnGroupInfosExtPtr, ColumnGroupInfos);
    DEFINE_BYREF_RO_PROPERTY(TRefCountedColumnMetaPtr, ColumnMeta);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::NProto::TMiscExt, Misc);
    DEFINE_BYREF_RO_PROPERTY(TTableSchemaPtr, ChunkSchema);
    DEFINE_BYREF_RO_PROPERTY(TNameTablePtr, ChunkNameTable);
    DEFINE_BYREF_RO_PROPERTY(TSharedRange<TUnversionedRow>, BlockLastKeys);
    DEFINE_BYREF_RO_PROPERTY(std::vector<THunkChunkRef>, HunkChunkRefs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<THunkChunkMeta>, HunkChunkMetas);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NTableClient::NProto::TColumnarStatisticsExt>, ColumnarStatisticsExt);

public:

    explicit TColumnarChunkMeta(const NChunkClient::NProto::TChunkMeta& chunkMeta);

    virtual i64 GetMemoryUsage() const;

    // Free space if there is prepared meta.
    void ClearColumnMeta();

private:
    i64 BlockLastKeysSize_;
};

DEFINE_REFCOUNTED_TYPE(TColumnarChunkMeta)

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr GetTableSchema(const NChunkClient::NProto::TChunkMeta& chunkMeta);

int GetCommonKeyPrefix(const TKeyColumns& lhs, const TKeyColumns& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
