#include "chunk_visitor.h"

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TChunkVisitorBase::TChunkVisitorBase(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList)
    : Bootstrap_(bootstrap)
    , ChunkList_(chunkList)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
}

TFuture<TYsonString> TChunkVisitorBase::Run()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto callbacks = CreatePreemptableChunkTraverserCallbacks(
        Bootstrap_,
        NCellMaster::EAutomatonThreadQueue::ChunkStatisticsTraverser);
    TraverseChunkTree(
        std::move(callbacks),
        this,
        ChunkList_);

    return Promise_;
}

void TChunkVisitorBase::OnFinish(const TError& error)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (error.IsOK()) {
        OnSuccess();
    } else {
        Promise_.Set(TError("Error traversing chunk tree") << error);
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkIdsAttributeVisitor::TChunkIdsAttributeVisitor(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList)
    : TChunkVisitorBase(bootstrap, chunkList)
    , Writer_(&Stream_)
{
    Writer_.OnBeginList();
}

bool TChunkIdsAttributeVisitor::OnChunk(
    TChunk* chunk,
    i64 /*rowIndex*/,
    const TReadLimit& /*startLimit*/,
    const TReadLimit& /*endLimit*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Writer_.OnListItem();
    Writer_.OnStringScalar(ToString(chunk->GetId()));

    return true;
}

void TChunkIdsAttributeVisitor::OnSuccess()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Writer_.OnEndList();
    Writer_.Flush();
    Promise_.Set(TYsonString(Stream_.Str()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
