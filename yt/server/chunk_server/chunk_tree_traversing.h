#pragma once
#include "public.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_reader.pb.h>
#include <server/cell_master/public.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NChunkServer {

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
    i64 lowerBound,
    TNullable<i64> upperBound);

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkProcessorPtr processor,
    const TChunkList* root,
    const NTableClient::NProto::TKey& lowerBound,
    TNullable<NTableClient::NProto::TKey> upperBound);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
