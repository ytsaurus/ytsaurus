#pragma once

#include "chunk.h"
#include "chunk_tree_statistics.h"
#include "chunk_tree_traverser.h"
#include "public.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkVisitorBase
    : public IChunkVisitor
{
public:
    TFuture<NYson::TYsonString> Run();

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TChunkLists ChunkLists_;

    TPromise<NYson::TYsonString> Promise_ = NewPromise<NYson::TYsonString>();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TChunkVisitorBase(
        NCellMaster::TBootstrap* bootstrap,
        const TChunkLists& chunkLists);

    void OnFinish(const TError& error) override;
    virtual void OnSuccess() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkIdsAttributeVisitor
    : public TChunkVisitorBase
{
public:
    TChunkIdsAttributeVisitor(
        NCellMaster::TBootstrap* bootstrap,
        const TChunkLists& chunkLists);

private:
    TStringStream Stream_;
    NYson::TBufferedBinaryYsonWriter Writer_;

    bool OnChunk(
        TChunk* chunk,
        TChunkList* /*parent*/,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/,
        const TChunkViewModifier* /*modifier*/) override;

    bool OnChunkView(TChunkView* /*chunkView*/) override;

    bool OnDynamicStore(
        TDynamicStore* /*dynamicStore*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override;

    void OnSuccess() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKeyExtractor>
TFuture<NYson::TYsonString> ComputeChunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists,
    TKeyExtractor keyExtractor);

template <class TKeyExtractor, class TKeyFormatter>
TFuture<NYson::TYsonString> ComputeChunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists,
    TKeyExtractor keyExtractor,
    TKeyFormatter keyFormatter);

////////////////////////////////////////////////////////////////////////////////

TFuture<NYson::TYsonString> ComputeHunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkLists& chunkLists);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_VISITOR_INL_H
#include "chunk_visitor-inl.h"
#undef CHUNK_VISITOR_INL_H

