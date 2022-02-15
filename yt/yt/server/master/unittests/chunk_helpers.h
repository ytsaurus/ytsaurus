#pragma once

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_view.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NChunkServer::NTesting {

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateId(NCypressClient::EObjectType type);

NTableClient::TUnversionedOwningRow BuildKey(const TString& yson);

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children);

void DetachFromChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children,
    EChunkDetachPolicy policy);

class TChunkGeneratorBase
    : public ::testing::Test
{
public:
    TChunk* CreateChunk(
        i64 rowCount,
        i64 compressedDataSize,
        i64 uncompressedDataSize,
        i64 dataWeight,
        NTableClient::TLegacyOwningKey minKey = {},
        NTableClient::TLegacyOwningKey maxKey = {},
        EChunkType chunkType = EChunkType::Table);

    TChunk* CreateUnconfirmedChunk();
    TChunk* CreateJournalChunk(bool sealed, bool overlayed);

    TChunkList* CreateChunkList(EChunkListKind kind = EChunkListKind::Static);

    TChunkView* CreateChunkView(
        TChunk* underlyingChunk,
        NTableClient::TLegacyOwningKey lowerLimit,
        NTableClient::TLegacyOwningKey upperLimit);

    void ConfirmChunk(
        TChunk* chunk,
        i64 rowCount,
        i64 compressedDataSize,
        i64 uncompressedDataSize,
        i64 dataWeight,
        NTableClient::TLegacyOwningKey minKey = {},
        NTableClient::TLegacyOwningKey maxKey = {});

private:
    std::vector<std::unique_ptr<TChunkTree>> CreatedObjects_;
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::TComparator MakeComparator(int keyLength);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NTesting
