#include "chunk_helpers.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/server/master/chunk_server/helpers.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/proto/chunk_meta.pb.h>

namespace NYT::NChunkServer::NTesting {

////////////////////////////////////////////////////////////////////////////////

TGuid GenerateId(NCypressClient::EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, 0, counter++, 0);
}

TChunk* TChunkGeneratorBase::CreateChunk(
    i64 rowCount,
    i64 compressedDataSize,
    i64 uncompressedDataSize,
    i64 dataWeight,
    NTableClient::TOwningKey minKey,
    NTableClient::TOwningKey maxKey,
    EChunkType chunkType)
{
    auto chunk = std::make_unique<TChunk>(GenerateId(NCypressClient::EObjectType::Chunk));
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

    chunk->Confirm(&chunkInfo, &chunkMeta);

    auto ptr = chunk.get();
    CreatedObjects_.push_back(std::move(chunk));
    return ptr;
}

TChunk* TChunkGeneratorBase::CreateUnconfirmedChunk(EChunkType chunkType)
{
    auto chunk = std::make_unique<TChunk>(GenerateId(NCypressClient::EObjectType::Chunk));
    chunk->RefObject();

    auto ptr = chunk.get();
    CreatedObjects_.push_back(std::move(chunk));
    return ptr;
}

TChunkList* TChunkGeneratorBase::CreateChunkList(EChunkListKind kind)
{
    auto chunkList = std::make_unique<TChunkList>(GenerateId(NCypressClient::EObjectType::ChunkList));
    chunkList->SetKind(kind);
    chunkList->RefObject();

    auto ptr = chunkList.get();
    CreatedObjects_.push_back(std::move(chunkList));
    return ptr;
}

TChunkView* TChunkGeneratorBase::CreateChunkView(
    TChunk* underlyingChunk,
    NTableClient::TOwningKey lowerLimit,
    NTableClient::TOwningKey upperLimit)
{
    NChunkClient::TReadRange readRange{
        NChunkClient::TReadLimit(lowerLimit), NChunkClient::TReadLimit(upperLimit)};
    auto chunkView = std::make_unique<TChunkView>(GenerateId(NCypressClient::EObjectType::ChunkView));

    chunkView->SetUnderlyingChunk(underlyingChunk);
    chunkView->SetReadRange(readRange);

    chunkView->RefObject();
    underlyingChunk->RefObject();

    auto ptr = chunkView.get();
    CreatedObjects_.push_back(std::move(chunkView));
    return ptr;
}

void TChunkGeneratorBase::ConfirmChunk(
    TChunk *chunk,
    i64 rowCount,
    i64 compressedDataSize,
    i64 uncompressedDataSize,
    i64 dataWeight,
    NTableClient::TOwningKey minKey,
    NTableClient::TOwningKey maxKey)
{
    auto* donorChunk = CreateChunk(
        rowCount,
        compressedDataSize,
        uncompressedDataSize,
        dataWeight,
        minKey,
        maxKey);

    chunk->Confirm(&donorChunk->ChunkInfo(), &donorChunk->ChunkMeta());
}

NTableClient::TUnversionedOwningRow BuildKey(const TString& yson)
{
    return NTableClient::YsonToKey(yson);
}

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    NChunkServer::AttachToChunkList(
        chunkList,
        children.data(),
        children.data() + children.size());
}

void DetachFromChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    NChunkServer::DetachFromChunkList(
        chunkList,
        children.data(),
        children.data() + children.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NTesting
