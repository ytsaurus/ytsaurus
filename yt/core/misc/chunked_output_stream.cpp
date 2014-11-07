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
}

TChunkedOutputStream::~TChunkedOutputStream() throw()
{ }

std::vector<TSharedRef> TChunkedOutputStream::FlushBuffer()
{
    CompleteChunks.push_back(TSharedRef::FromBlob<TChunkedOutputStreamTag>(std::move(IncompleteChunk)));

    YASSERT(IncompleteChunk.IsEmpty());
    CompleteSize = 0;

    return std::move(CompleteChunks);
}

size_t TChunkedOutputStream::GetSize() const
{
    return CompleteSize + IncompleteChunk.Size();
}

size_t TChunkedOutputStream::GetCapacity() const
{
    return CompleteSize + IncompleteChunk.Capacity();
}

void TChunkedOutputStream::DoWrite(const void* buffer, size_t length)
{
    if (!IncompleteChunk.Capacity()) {
        IncompleteChunk.Reserve(CurrentReserveSize);
    }

    const auto spaceAvailable = std::min(length, IncompleteChunk.Capacity() - IncompleteChunk.Size());
    const auto spaceRequired = length - spaceAvailable;

    IncompleteChunk.Append(buffer, spaceAvailable);

    if (spaceRequired) {
        YASSERT(IncompleteChunk.Size() == IncompleteChunk.Capacity());

        CompleteSize += IncompleteChunk.Size();
        CompleteChunks.push_back(TSharedRef::FromBlob<TChunkedOutputStreamTag>(std::move(IncompleteChunk)));

        YASSERT(IncompleteChunk.IsEmpty());

        CurrentReserveSize = std::min(2 * CurrentReserveSize, MaxReserveSize);

        IncompleteChunk.Reserve(std::max(RoundUpToPage(spaceRequired), CurrentReserveSize));
        IncompleteChunk.Append(static_cast<const char*>(buffer) + spaceAvailable, spaceRequired);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
