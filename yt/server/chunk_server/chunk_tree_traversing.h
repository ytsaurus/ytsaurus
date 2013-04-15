#pragma once

#include "private.h"

#include <ytlib/misc/error.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/input_chunk.pb.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETraversingError,
    (Fatal)

    // Indicates that subsequent traversing attempt may succeed.
    // This typically happens when an optimistic chunk tree locking fails.
    (Retriable)
);

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
