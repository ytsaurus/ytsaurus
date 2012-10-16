#pragma once
#include "public.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_reader.pb.h>
#include <server/cell_master/public.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETraversingError,
    (Fatal)

    // Retriable error means that subsequent traversing attempt may be successul.
    // E.g. chunk list version has changed.
    (Retriable)
);

////////////////////////////////////////////////////////////////////////////////

struct IChunkProcessor
    : public virtual TRefCounted
{
    virtual void ProcessChunk(
        const TChunk* chunk, 
        const NTableClient::NProto::TReadLimit& startLimit,
        const NTableClient::NProto::TReadLimit& endLimit) = 0;
    virtual void OnError(const TError& error) = 0;
    virtual void OnComplete() = 0;

};

// Move to private.h (or public.h?)
typedef TIntrusivePtr<IChunkProcessor> IChunkProcessorPtr;

////////////////////////////////////////////////////////////////////////////////

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkProcessorPtr processor,
    const TChunkList* root,
    const NTableClient::NProto::TReadLimit& lowerBound,
    const NTableClient::NProto::TReadLimit& upperBound);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
