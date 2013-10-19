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

    //! Returns a pointer to a continious memory chunk of given size in the stream buffer.
    //! Do not forget to call Skip after use.
    char* Allocate(size_t size);
    void Skip(size_t size);

    virtual void DoWrite(const void* buf, size_t len) override;

private:
    const size_t MaxReserveSize;
    size_t CurrentReserveSize;

    size_t CompleteSize;

    TBlob IncompleteChunk;
    std::vector<TSharedRef> CompleteChunks;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
