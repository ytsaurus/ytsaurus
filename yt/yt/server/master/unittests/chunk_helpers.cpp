#include "chunk_helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NChunkServer::NTesting {

using namespace NObjectClient;
using namespace NTableClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateId(EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, TCellTag(0), counter++, 0);
}

TChunk* TChunkGeneratorBase::CreateChunk(
    i64 rowCount,
    i64 compressedDataSize,
    i64 uncompressedDataSize,
    i64 dataWeight,
    NTableClient::TLegacyOwningKey minKey,
    NTableClient::TLegacyOwningKey maxKey,
    EChunkType chunkType)
{
    auto chunk = TPoolAllocator::New<TChunk>(GenerateId(EObjectType::Chunk));
    chunk->RefObject();

    NChunkClient::NProto::TChunkMeta chunkMeta;
    chunkMeta.set_type(static_cast<int>(chunkType));

    NChunkClient::NProto::TMiscExt miscExt;
    miscExt.set_row_count(rowCount);
    miscExt.set_uncompressed_data_size(uncompressedDataSize);
    miscExt.set_compressed_data_size(compressedDataSize);
    miscExt.set_data_weight(dataWeight);
    SetProtoExtension(chunkMeta.mutable_extensions(), miscExt);

    NTableClient::NProto::TBoundaryKeysExt boundaryKeysExt;
    ToProto(boundaryKeysExt.mutable_min(), minKey);
    ToProto(boundaryKeysExt.mutable_max(), maxKey);
    SetProtoExtension(chunkMeta.mutable_extensions(), boundaryKeysExt);

    NChunkClient::NProto::TChunkInfo chunkInfo;

    chunk->Confirm(chunkInfo, chunkMeta);

    auto ptr = chunk.get();
    CreatedObjects_.push_back(std::move(chunk));
    return ptr;
}

TChunk* TChunkGeneratorBase::CreateUnconfirmedChunk()
{
    auto chunk = TPoolAllocator::New<TChunk>(GenerateId(EObjectType::Chunk));
    chunk->RefObject();

    auto ptr = chunk.get();
    CreatedObjects_.push_back(std::move(chunk));
    return ptr;
}

TChunk* TChunkGeneratorBase::CreateJournalChunk(bool sealed, bool overlayed)
{
    auto chunk = TPoolAllocator::New<TChunk>(GenerateId(EObjectType::JournalChunk));
    chunk->SetOverlayed(overlayed);
    chunk->RefObject();

    NChunkClient::NProto::TChunkMeta chunkMeta;
    chunkMeta.set_type(ToProto<int>(EChunkType::Journal));

    NChunkClient::NProto::TMiscExt miscExt;
    SetProtoExtension(chunkMeta.mutable_extensions(), miscExt);

    NChunkClient::NProto::TChunkInfo chunkInfo;

    chunk->Confirm(chunkInfo, chunkMeta);

    if (sealed) {
        NChunkClient::NProto::TChunkSealInfo sealInfo;
        sealInfo.set_row_count(100);
        sealInfo.set_uncompressed_data_size(100);
        sealInfo.set_compressed_data_size(100);
        chunk->Seal(sealInfo);
    }

    auto ptr = chunk.get();
    CreatedObjects_.push_back(std::move(chunk));
    return ptr;
}

TChunkList* TChunkGeneratorBase::CreateChunkList(EChunkListKind kind)
{
    auto chunkList = TPoolAllocator::New<TChunkList>(GenerateId(EObjectType::ChunkList));
    chunkList->SetKind(kind);
    chunkList->RefObject();

    auto ptr = chunkList.get();
    CreatedObjects_.push_back(std::move(chunkList));
    return ptr;
}

TChunkView* TChunkGeneratorBase::CreateChunkView(
    TChunk* underlyingChunk,
    NTableClient::TLegacyOwningKey lowerLimit,
    NTableClient::TLegacyOwningKey upperLimit)
{
    NChunkClient::TLegacyReadRange readRange{
        NChunkClient::TLegacyReadLimit(lowerLimit), NChunkClient::TLegacyReadLimit(upperLimit)};
    auto chunkView = TPoolAllocator::New<TChunkView>(GenerateId(EObjectType::ChunkView));

    chunkView->SetUnderlyingTree(underlyingChunk);
    chunkView->Modifier().SetReadRange(readRange);

    chunkView->RefObject();
    underlyingChunk->RefObject();

    auto ptr = chunkView.get();
    CreatedObjects_.push_back(std::move(chunkView));
    return ptr;
}

void TChunkGeneratorBase::SetUp()
{
    SetupMasterSmartpointers();
}

void TChunkGeneratorBase::TearDown()
{
    ResetMasterSmartpointers();
}

void TChunkGeneratorBase::ConfirmChunk(
    TChunk *chunk,
    i64 rowCount,
    i64 compressedDataSize,
    i64 uncompressedDataSize,
    i64 dataWeight,
    NTableClient::TLegacyOwningKey minKey,
    NTableClient::TLegacyOwningKey maxKey)
{
    auto* donorChunk = CreateChunk(
        rowCount,
        compressedDataSize,
        uncompressedDataSize,
        dataWeight,
        minKey,
        maxKey);

    NChunkClient::NProto::TChunkInfo chunkInfo;
    chunkInfo.set_disk_space(donorChunk->GetDiskSpace());

    chunk->Confirm(
        chunkInfo,
        ToProto<NChunkClient::NProto::TChunkMeta>(donorChunk->ChunkMeta()));
}

NTableClient::TUnversionedOwningRow BuildKey(const TString& yson)
{
    return NTableClient::YsonToKey(yson);
}

////////////////////////////////////////////////////////////////////////////////

TComparator MakeComparator(int keyLength)
{
    return TComparator(std::vector<ESortOrder>(keyLength, ESortOrder::Ascending));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NTesting
