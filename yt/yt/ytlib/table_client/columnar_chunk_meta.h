#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "hunks.h"

#include <yt/yt/ytlib/columnar_chunk_format/compressed_block_last_keys.h>

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
    DEFINE_BYREF_RO_PROPERTY(std::vector<THunkChunkRef>, HunkChunkRefs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<THunkChunkMeta>, HunkChunkMetas);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NTableClient::NProto::TColumnarStatisticsExt>, ColumnarStatisticsExt);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NTableClient::NProto::TLargeColumnarStatisticsExt>, LargeColumnarStatisticsExt);

public:
    explicit TColumnarChunkMeta(const NChunkClient::NProto::TChunkMeta& chunkMeta, bool compressBlockLastKeys = false);

    virtual i64 GetMemoryUsage() const;

    const TSharedRange<TUnversionedRow>& BlockLastKeys() const;

    TKeyRef GetBlockLastKey(int index, std::vector<TUnversionedValue>* buffer) const;

    const NColumnarChunkFormat::IBlockLastKeys* GetCompressedBlockLastKeys() const;

private:
    TSharedRange<TUnversionedRow> BlockLastKeys_;
    std::unique_ptr<NColumnarChunkFormat::IBlockLastKeys> CompressedBlockLastKeys_;
    i64 BlockLastKeysSize_;
};

DEFINE_REFCOUNTED_TYPE(TColumnarChunkMeta)

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr GetTableSchema(const NChunkClient::NProto::TChunkMeta& chunkMeta);

int GetCommonKeyPrefix(const TKeyColumns& lhs, const TKeyColumns& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
