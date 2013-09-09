#pragma once

#include "private.h"

#include <core/misc/error.h>

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
     *  \note Return false to break off traversing.
     */
    virtual bool OnChunk(
        TChunk* chunk,
        const NChunkClient::NProto::TReadLimit& startLimit,
        const NChunkClient::NProto::TReadLimit& endLimit) = 0;

    virtual void OnError(const TError& error) = 0;

    virtual void OnFinish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const NChunkClient::NProto::TReadLimit& lowerBound = NChunkClient::NProto::TReadLimit(),
    const NChunkClient::NProto::TReadLimit& upperBound = NChunkClient::NProto::TReadLimit());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
