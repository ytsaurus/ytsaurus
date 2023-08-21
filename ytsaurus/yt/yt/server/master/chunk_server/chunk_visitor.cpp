#include "chunk_visitor.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TChunkVisitorBase::TChunkVisitorBase(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists)
    : Bootstrap_(bootstrap)
    , ChunkLists_(chunkLists)
{
    Bootstrap_->VerifyPersistentStateRead();
}

TFuture<TYsonString> TChunkVisitorBase::Run()
{
    Bootstrap_->VerifyPersistentStateRead();

    auto context = CreateAsyncChunkTraverserContext(
        Bootstrap_,
        NCellMaster::EAutomatonThreadQueue::ChunkStatisticsTraverser);
    TraverseChunkTree(
        std::move(context),
        this,
        ChunkLists_);

    return Promise_;
}

void TChunkVisitorBase::OnFinish(const TError& error)
{
    Bootstrap_->VerifyPersistentStateRead();

    if (error.IsOK()) {
        OnSuccess();
    } else {
        Promise_.Set(TError("Error traversing chunk tree") << error);
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkIdsAttributeVisitor::TChunkIdsAttributeVisitor(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists)
    : TChunkVisitorBase(bootstrap, chunkLists)
    , Writer_(&Stream_)
{
    Writer_.OnBeginList();
}

bool TChunkIdsAttributeVisitor::OnChunk(
    TChunk* chunk,
    TChunkList* /*parent*/,
    std::optional<i64> /*rowIndex*/,
    std::optional<int> /*tabletIndex*/,
    const TReadLimit& /*startLimit*/,
    const TReadLimit& /*endLimit*/,
    const TChunkViewModifier* /*modifier*/)
{
    Bootstrap_->VerifyPersistentStateRead();

    Writer_.OnListItem();
    Writer_.OnStringScalar(ToString(chunk->GetId()));

    return true;
}

bool TChunkIdsAttributeVisitor::OnChunkView(TChunkView* /*chunkView*/)
{
    return false;
}

bool TChunkIdsAttributeVisitor::OnDynamicStore(
    TDynamicStore* /*dynamicStore*/,
    std::optional<int> /*tabletIndex*/,
    const NChunkClient::TReadLimit& /*startLimit*/,
    const NChunkClient::TReadLimit& /*endLimit*/)
{
    return true;
}

void TChunkIdsAttributeVisitor::OnSuccess()
{
    Bootstrap_->VerifyPersistentStateRead();

    Writer_.OnEndList();
    Writer_.Flush();
    Promise_.Set(TYsonString(Stream_.Str()));
}

////////////////////////////////////////////////////////////////////////////////

class THunkStatisticsVisitor
    : public TChunkVisitorBase
{
public:
    using TChunkVisitorBase::TChunkVisitorBase;

private:
    int HunkChunkCount_ = 0;
    int StoreChunkCount_ = 0;
    i64 HunkCount_ = 0;
    i64 TotalHunkLength_ = 0;
    i64 ReferencedHunkCount_ = 0;
    i64 TotalReferencedHunkLength_ = 0;


    bool OnChunk(
        TChunk* chunk,
        TChunkList* /*parent*/,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/,
        const TChunkViewModifier* /*modifier*/) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        switch (chunk->GetChunkType()) {
            case EChunkType::Table:
                StoreChunkCount_ += 1;
                if (auto hunkChunkRefsExt = chunk->ChunkMeta()->FindExtension<NTableClient::NProto::THunkChunkRefsExt>()) {
                    for (const auto& protoRef : hunkChunkRefsExt->refs()) {
                        ReferencedHunkCount_ += protoRef.hunk_count();
                        TotalReferencedHunkLength_ += protoRef.total_hunk_length();
                    }
                }
                break;

            // TODO(aleksandra-zh): add journal chunks.
            case EChunkType::Hunk:
                if (auto hunkChunkMiscExt = chunk->ChunkMeta()->FindExtension<NTableClient::NProto::THunkChunkMiscExt>()) {
                    HunkChunkCount_ += 1;
                    HunkCount_ += hunkChunkMiscExt->hunk_count();
                    TotalHunkLength_ += hunkChunkMiscExt->total_hunk_length();
                }
                break;

            default:
                break;
        }

        return true;
    }

    bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        return false;
    }

    bool OnDynamicStore(
        TDynamicStore* /*dynamicStore*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        return true;
    }

    void OnSuccess() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto result = BuildYsonStringFluently()
            .BeginMap()
                .Item("hunk_chunk_count").Value(HunkChunkCount_)
                .Item("store_chunk_count").Value(StoreChunkCount_)
                .Item("hunk_count").Value(HunkCount_)
                .Item("total_hunk_length").Value(TotalHunkLength_)
                .Item("total_referenced_hunk_length").Value(TotalReferencedHunkLength_)
                .Item("referenced_hunk_count").Value(ReferencedHunkCount_)
            .EndMap();
        Promise_.Set(result);
    }
};

TFuture<TYsonString> ComputeHunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists)
{
    auto visitor = New<THunkStatisticsVisitor>(
        bootstrap,
        chunkLists);
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
