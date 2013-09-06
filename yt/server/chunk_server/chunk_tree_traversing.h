#pragma once

#include "private.h"

#include <ytlib/misc/error.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkVisitor
    : public virtual TRefCounted
{
    /*!
     *  \note Return |false| to terminate traversing.
     */
    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        const NChunkClient::NProto::TReadLimit& startLimit,
        const NChunkClient::NProto::TReadLimit& endLimit) = 0;

    virtual void OnError(const TError& error) = 0;

    virtual void OnFinish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkTraverserCallbacks
    : public virtual TRefCounted
{
    virtual IInvokerPtr GetInvoker() const = 0;

    virtual void OnPop(TChunkTree* node) = 0;
    
    virtual void OnPush(TChunkTree* node) = 0;

    virtual void OnShutdown(const std::vector<TChunkTree*>& nodes) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IChunkTraverserCallbacksPtr CreateTraverserCallbacks(
    NCellMaster::TBootstrap* bootstrap);

void TraverseChunkTree(
    IChunkTraverserCallbacksPtr bootstrap,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const NChunkClient::NProto::TReadLimit& lowerBound = NChunkClient::NProto::TReadLimit(),
    const NChunkClient::NProto::TReadLimit& upperBound = NChunkClient::NProto::TReadLimit());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
