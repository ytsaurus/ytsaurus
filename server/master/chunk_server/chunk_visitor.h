#pragma once

#include "chunk.h"
#include "chunk_tree_statistics.h"
#include "chunk_tree_traverser.h"
#include "public.h"

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkVisitorBase
    : public IChunkVisitor
{
public:
    TFuture<NYson::TYsonString> Run();

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    TChunkList* const ChunkList_;

    TPromise<NYson::TYsonString> Promise_ = NewPromise<NYson::TYsonString>();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TChunkVisitorBase(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList);

    virtual void OnFinish(const TError& error) override;
    virtual void OnSuccess() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkIdsAttributeVisitor
    : public TChunkVisitorBase
{
public:
    TChunkIdsAttributeVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList);

private:
    TStringStream Stream_;
    NYson::TBufferedBinaryYsonWriter Writer_;

    virtual bool OnChunk(
        TChunk* chunk,
        i64 /*rowIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override;

    virtual void OnSuccess() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKeyExtractor>
TFuture<NYson::TYsonString> ComputeChunkStatistics(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    TKeyExtractor keyExtractor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_VISITOR_INL_H
#include "chunk_visitor-inl.h"
#undef CHUNK_VISITOR_INL_H

