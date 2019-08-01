#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/chunk_view.h>

#include <yt/client/table_client/public.h>
#include <yt/client/table_client/unversioned_row.h>

namespace NYT::NChunkServer::NTesting {

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateId(NCypressClient::EObjectType type);

NTableClient::TUnversionedOwningRow BuildKey(const TString& yson);

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children);

void DetachFromChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children);

class TChunkGeneratorBase
    : public ::testing::Test
{
public:
    TChunk* CreateChunk(
        i64 rowCount,
        i64 compressedDataSize,
        i64 uncompressedDataSize,
        i64 dataWeight,
        NTableClient::TOwningKey minKey = {},
        NTableClient::TOwningKey maxKey = {},
        EChunkType chunkType = EChunkType::Table);

    TChunk* CreateUnconfirmedChunk(EChunkType chunkType = EChunkType::Table);

    TChunkList* CreateChunkList(EChunkListKind kind = EChunkListKind::Static);

    TChunkView* CreateChunkView(
        TChunk* underlyingChunk,
        NTableClient::TOwningKey lowerLimit,
        NTableClient::TOwningKey upperLimit);

    void ConfirmChunk(
        TChunk* chunk,
        i64 rowCount,
        i64 compressedDataSize,
        i64 uncompressedDataSize,
        i64 dataWeight,
        NTableClient::TOwningKey minKey = {},
        NTableClient::TOwningKey maxKey = {});

private:
    std::vector<std::unique_ptr<TChunkTree>> CreatedObjects_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NTesting
