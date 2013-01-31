#include "stdafx.h"

#include "chunked_output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TChunkedOutputStreamTag { };

TChunkedOutputStream::TChunkedOutputStream(size_t maxReserveSize, size_t initialReserveSize)
    : MaxReserveSize(RoundUpToPage(maxReserveSize))
    , CurrentReserveSize(RoundUpToPage(initialReserveSize))
    , CompleteSize(0)
{
    if (CurrentReserveSize > MaxReserveSize) {
        CurrentReserveSize = MaxReserveSize;
    }

    YCHECK(MaxReserveSize > 0);

    IncompleteChunk.reserve(CurrentReserveSize);
}

TChunkedOutputStream::~TChunkedOutputStream() throw()
{ }

std::vector<TSharedRef> TChunkedOutputStream::FlushBuffer()
{
    CompleteChunks.push_back(TSharedRef::FromBlob<TChunkedOutputStreamTag>(std::move(IncompleteChunk)));

    YASSERT(IncompleteChunk.empty());
    CompleteSize = 0;
    IncompleteChunk.reserve(CurrentReserveSize);

    return std::move(CompleteChunks);
}

size_t TChunkedOutputStream::GetSize() const
{
    return CompleteSize + IncompleteChunk.size();
}

size_t TChunkedOutputStream::GetCapacity() const
{
    return CompleteSize + IncompleteChunk.capacity();
}

void TChunkedOutputStream::DoWrite(const void* buffer, size_t length)
{
    const auto spaceAvailable = std::min(length, IncompleteChunk.capacity() - IncompleteChunk.size());
    const auto spaceRequired = length - spaceAvailable;

    AppendToBlob(IncompleteChunk, buffer, spaceAvailable);

    if (spaceRequired) {
        YASSERT(IncompleteChunk.size() == IncompleteChunk.capacity());

        CompleteSize += IncompleteChunk.size();
        CompleteChunks.push_back(TSharedRef::FromBlob<TChunkedOutputStreamTag>(std::move(IncompleteChunk)));

        YASSERT(IncompleteChunk.empty());

        CurrentReserveSize = std::min(2 * CurrentReserveSize, MaxReserveSize);

        IncompleteChunk.reserve(std::max(RoundUpToPage(spaceRequired), CurrentReserveSize));
        AppendToBlob(IncompleteChunk, static_cast<const char*>(buffer) + spaceAvailable, spaceRequired);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
