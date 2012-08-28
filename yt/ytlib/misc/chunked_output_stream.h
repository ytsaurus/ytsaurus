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
    TChunkedOutputStream(int minChunkSize);
    ~TChunkedOutputStream() throw();

    std::vector<TSharedRef> FlushBuffer();
    size_t GetSize() const;

private:
    const int MinChunkSize;
    const size_t CompleteSize;

    std::vector<TSharedRef> CompleteChunks;
    TBlob IncompleteChunk;

    void DoWrite(const void* buf, size_t len);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT