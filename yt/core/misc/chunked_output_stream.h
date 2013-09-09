#pragma once

#include "common.h"
#include "ref.h"

#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkedOutputStream
    : public TOutputStream
{
public:
    TChunkedOutputStream(
        size_t maxReserveSize = 64 * 1024,
        size_t initialReserveSize = 4 * 1024);

    ~TChunkedOutputStream() throw();

    std::vector<TSharedRef> FlushBuffer();
    size_t GetSize() const;
    size_t GetCapacity() const;

private:
    virtual void DoWrite(const void* buf, size_t len) override;

    const size_t MaxReserveSize;
    size_t CurrentReserveSize;

    size_t CompleteSize;

    TBlob IncompleteChunk;
    std::vector<TSharedRef> CompleteChunks;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
