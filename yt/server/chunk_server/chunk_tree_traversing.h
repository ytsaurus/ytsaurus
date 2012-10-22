#pragma once

#include "private.h"

#include <ytlib/misc/error.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_reader.pb.h>

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
    virtual void OnChunk(
        const TChunk* chunk, 
        const NTableClient::NProto::TReadLimit& startLimit,
        const NTableClient::NProto::TReadLimit& endLimit) = 0;

    virtual void OnError(const TError& error) = 0;

    virtual void OnFinish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkVisitorPtr visitor,
    const TChunkList* root,
    const NTableClient::NProto::TReadLimit& lowerBound,
    const NTableClient::NProto::TReadLimit& upperBound);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
