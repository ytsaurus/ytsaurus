#pragma once

#include "public.h"

#include <util/stream/fwd.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkedMemoryPoolOutput
    : public IZeroCopyOutput
{
public:
    static constexpr size_t DefaultChunkSize = 36_KB - 512;

public:
    explicit TChunkedMemoryPoolOutput(TChunkedMemoryPool* pool, size_t chunkSize = DefaultChunkSize);

    std::vector<TRef> FinishAndGetRefs();

private:
    virtual size_t DoNext(void** ptr) override;
    virtual void DoUndo(size_t size) override;

private:
    TChunkedMemoryPool* const Pool_;
    const size_t ChunkSize_;
    char* Begin_ = nullptr;
    char* End_ = nullptr;
    std::vector<TRef> Refs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

