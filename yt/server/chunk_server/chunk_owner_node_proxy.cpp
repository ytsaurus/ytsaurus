#include "stdafx.h"

#include "chunk_owner_node_proxy.h"
#include "chunk.h"
#include "chunk_manager.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/erasure/codec.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;
using NChunkClient::NProto::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

TFetchChunkVisitor::TFetchChunkVisitor(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    TCtxFetchPtr context,
    const TChannel& channel)
    : Bootstrap(bootstrap)
    , ChunkList(chunkList)
    , Context(context)
    , Channel(channel)
    , NodeDirectoryBuilder(context->Response().mutable_node_directory())
    , SessionCount(0)
    , Completed(false)
    , Finished(false)
{
    if (!Context->Request().fetch_all_meta_extensions()) {
        FOREACH (int tag, Context->Request().extension_tags()) {
            ExtensionTags.insert(tag);
        }
    }
}

void TFetchChunkVisitor::StartSession(
    const TReadLimit& lowerBound,
    const TReadLimit& upperBound)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ++SessionCount;

    TraverseChunkTree(
        Bootstrap,
        this,
        ChunkList,
        lowerBound,
        upperBound);
}

void TFetchChunkVisitor::Complete()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(!Completed);

    Completed = true;
    if (SessionCount == 0 && !Finished) {
        Reply();
    }
}

void TFetchChunkVisitor::Reply()
{
    Context->SetResponseInfo("ChunkCount: %d", Context->Response().chunks_size());
    Context->Reply();
    Finished = true;
}

bool TFetchChunkVisitor::OnChunk(
    TChunk* chunk,
    const TReadLimit& startLimit,
    const TReadLimit& endLimit) override
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto chunkManager = Bootstrap->GetChunkManager();

    if (!chunk->IsConfirmed()) {
        ReplyError(TError("Cannot fetch a table containing an unconfirmed chunk %s",
            ~ToString(chunk->GetId())));
        return false;
    }

    if (!chunk->IsAvailable())  {
        if (Context->Request().skip_unavailable_chunks()) {
            // Just ignore this chunk.
            return true;
        } else {
            ReplyError(TError("Chunk %s is unavailable",
                ~ToString(chunk->GetId())));
            return false;
        }
    }

    auto* inputChunk = Context->Response().add_chunks();

    if (!Channel.IsUniversal()) {
        *inputChunk->mutable_channel() = Channel.ToProto();
    }

    auto erasureCodecId = chunk->GetErasureCodec();
    int firstParityPartIndex =
        erasureCodecId == NErasure::ECodec::None
        ? 1 // makes no sense anyway
        : NErasure::GetCodec(erasureCodecId)->GetDataBlockCount();

    auto replicas = chunk->GetReplicas();
    FOREACH (auto replica, replicas) {
        if (replica.GetIndex() < firstParityPartIndex) {
            NodeDirectoryBuilder.Add(replica);
            inputChunk->add_replicas(NYT::ToProto<ui32>(replica));
        }
    }

    ToProto(inputChunk->mutable_chunk_id(), chunk->GetId());
    inputChunk->set_erasure_codec(erasureCodecId);

    if (Context->Request().fetch_all_meta_extensions()) {
        *inputChunk->mutable_extensions() = chunk->ChunkMeta().extensions();
    } else {
        FilterProtoExtensions(
            inputChunk->mutable_extensions(),
            chunk->ChunkMeta().extensions(),
            ExtensionTags);
    }

    // Try to keep responses small -- avoid producing redundant limits.
    if (IsNontrivial(startLimit)) {
        *inputChunk->mutable_start_limit() = startLimit;
    }
    if (IsNontrivial(endLimit)) {
        *inputChunk->mutable_end_limit() = endLimit;
    }

    return true;
}

bool TFetchChunkVisitor::IsNontrivial(const TReadLimit& limit)
{
    return limit.has_row_index() ||
           limit.has_key() ||
           limit.has_chunk_index() ||
           limit.has_offset();
}

void TFetchChunkVisitor::OnError(const TError& error) override
{
    VERIFY_THREAD_AFFINITY(StateThread);

    --SessionCount;
    YCHECK(SessionCount >= 0);

    ReplyError(error);
}

void TFetchChunkVisitor::OnFinish() override
{
    VERIFY_THREAD_AFFINITY(StateThread);

    --SessionCount;
    YCHECK(SessionCount >= 0);

    if (Completed && !Finished && SessionCount == 0) {
        Reply();
    }
}

void TFetchChunkVisitor::ReplyError(const TError& error)
{
    if (Finished)
        return;

    Context->Reply(error);
    Finished = true;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkVisitorBase
    : public IChunkVisitor
{
public:
    TAsyncError Run()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        TraverseChunkTree(
            Bootstrap,
            this,
            ChunkList);

        return Promise;
    }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    IYsonConsumer* Consumer;
    TChunkList* ChunkList;
    TPromise<TError> Promise;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TChunkVisitorBase(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : Bootstrap(bootstrap)
        , Consumer(consumer)
        , ChunkList(chunkList)
        , Promise(NewPromise<TError>())
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    }

    virtual void OnError(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        Promise.Set(TError("Error traversing chunk tree") << error);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkIdsAttributeVisitor
    : public TChunkVisitorBase
{
public:
    TChunkIdsAttributeVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
    {
        Consumer->OnBeginList();
    }

    virtual bool OnChunk(
        TChunk* chunk,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(startLimit);
        UNUSED(endLimit);

        Consumer->OnListItem();
        Consumer->OnStringScalar(ToString(chunk->GetId()));

        return true;
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        Consumer->OnEndList();
        Promise.Set(TError());
    }
};

TAsyncError GetChunkIdsAttribute(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    IYsonConsumer* consumer)
{
    auto visitor = New<TChunkIdsAttributeVisitor>(
        bootstrap,
        const_cast<TChunkList*>(chunkList),
        consumer);
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TCodecStatisticsAttributeVisitor
    : public TChunkVisitorBase
{
public:
    TCodecStatisticsAttributeVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
    { }

    virtual bool OnChunk(
        TChunk* chunk,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(startLimit);
        UNUSED(endLimit);

        const auto& chunkMeta = chunk->ChunkMeta();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta.extensions());

        CodecInfo[NCompression::ECodec(miscExt.compression_codec())].Accumulate(chunk->GetStatistics());
        return true;
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        BuildYsonFluently(Consumer)
            .DoMapFor(CodecInfo, [=] (TFluentMap fluent, const TCodecInfoMap::value_type& pair) {
                const auto& statistics = pair.second;
                // TODO(panin): maybe use here the same method as in attributes
                fluent
                    .Item(FormatEnum(pair.first)).BeginMap()
                        .Item("chunk_count").Value(statistics.ChunkCount)
                        .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
                        .Item("compressed_data_size").Value(statistics.CompressedDataSize)
                    .EndMap();
            });
        Promise.Set(TError());
    }

private:
    typedef yhash_map<NCompression::ECodec, TChunkTreeStatistics> TCodecInfoMap;
    TCodecInfoMap CodecInfo;

};

TAsyncError GetCodecStatisticsAttribute(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    IYsonConsumer* consumer)
{
    auto visitor = New<TCodecStatisticsAttributeVisitor>(
        bootstrap,
        const_cast<TChunkList*>(chunkList),
        consumer);
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // NChunkServer
} // NYT