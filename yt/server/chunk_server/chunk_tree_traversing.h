#pragma once

#include "private.h"

#include <core/misc/error.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/chunk_client/read_limit.h>

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
        const NChunkClient::TReadLimit& startLimit,
        const NChunkClient::TReadLimit& endLimit) = 0;

    virtual void OnError(const TError& error) = 0;

    virtual void OnFinish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkTraverserCallbacks
    : public virtual TRefCounted
{
    virtual bool IsPreemptable() const = 0;

    virtual IInvokerPtr GetInvoker() const = 0;

    virtual void OnPop(TChunkTree* node) = 0;
    
    virtual void OnPush(TChunkTree* node) = 0;

    virtual void OnShutdown(const std::vector<TChunkTree*>& nodes) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IChunkTraverserCallbacksPtr CreatePreemptableChunkTraverserCallbacks(
    NCellMaster::TBootstrap* bootstrap);

IChunkTraverserCallbacksPtr GetNonpreemptableChunkTraverserCallbacks();

void TraverseChunkTree(
    IChunkTraverserCallbacksPtr callbacks,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const NChunkClient::TReadLimit& lowerLimit = NChunkClient::TReadLimit(),
    const NChunkClient::TReadLimit& upperLimit = NChunkClient::TReadLimit());

void EnumerateChunksInChunkTree(
    TChunkList* root,
    std::vector<TChunk*>* chunks,
    const NChunkClient::TReadLimit& lowerBound = NChunkClient::TReadLimit(),
    const NChunkClient::TReadLimit& upperBound = NChunkClient::TReadLimit());

std::vector<TChunk*> EnumerateChunksInChunkTree(
    TChunkList* root,
    const NChunkClient::TReadLimit& lowerBound = NChunkClient::TReadLimit(),
    const NChunkClient::TReadLimit& upperBound = NChunkClient::TReadLimit());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
