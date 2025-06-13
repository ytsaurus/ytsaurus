#pragma once

#include "public.h"

#include <yt/yt/library/s3/client.h>

#include <arrow/io/interfaces.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

struct IChunkMetaGenerator
    : public TRefCounted
{
    //! Must be called before calling any other methods.
    //! The only method allowed to make asynchronous calls.
    virtual void Generate() = 0;
    
    //! Static accessors to the generated objects.
    virtual TRefCountedChunkMetaPtr GetChunkMeta() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkMetaGenerator)

////////////////////////////////////////////////////////////////////////////

IChunkMetaGeneratorPtr CreateArrowChunkMetaGenerator(
    EChunkFormat chunkFormat,
    const std::shared_ptr<arrow::io::RandomAccessFile>& chunkFile);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient