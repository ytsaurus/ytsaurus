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
        int maxReserveSize = 64 * 1024, 
        int initialReserveSize = 1024);

    ~TChunkedOutputStream() throw();

    std::vector<TSharedRef> FlushBuffer();
    size_t GetSize() const;
    size_t GetCapacity() const;

private:
    const int MaxReserveSize;

    size_t CompleteSize;
    int CurrentReserveSize;

    std::vector<TSharedRef> CompleteChunks;
    TBlob IncompleteChunk;

    void DoWrite(const void* buf, size_t len);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT