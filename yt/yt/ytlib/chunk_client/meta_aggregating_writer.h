#pragma once

#include "chunk_writer.h"
#include "public.h"
#include "config.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IMetaAggregatingWriter
    : public IChunkWriter
{
    virtual void AbsorbMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId) = 0;

    virtual const TDeferredChunkMetaPtr& GetChunkMeta() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMetaAggregatingWriterPtr CreateMetaAggregatingWriter(
    IChunkWriterPtr underlyingWriter,
    TMetaAggregatingWriterOptionsPtr options);

DEFINE_REFCOUNTED_TYPE(IMetaAggregatingWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
