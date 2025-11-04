#pragma once

#include "public.h"

#include <util/stream/output.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

struct TBuildIndexOptions
{
    i64 ChunkSize = DefaultChunkSize;
    i64 BlockSize = DefaultBlockSize;
    i64 IndexSegmentSize = DefaultIndexSegmentSize;
    double IndexSizeFactor = DefaultIndexSizeFactor;
};

struct IIndexBuilderCallbacks
{
    virtual ~IIndexBuilderCallbacks() = default;

    virtual void OnProgress(i64 bytesIndexed, i64 bytesTotal) = 0;
};

void BuildIndex(
    ISequentialReader* reader,
    IOutputStream* output,
    const TBuildIndexOptions& options,
    IIndexBuilderCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
