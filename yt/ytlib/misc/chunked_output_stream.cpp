#include "stdafx.h"

#include "chunked_output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChunkedOutputStream::TChunkedOutputStream(int maxReserveSize, int initialReserveSize)
    : MaxReserveSize(MaxReserveSize)
    , CurrentReserveSize(initialReserveSize)
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
    CompleteChunks.push_back(TSharedRef(MoveRV(IncompleteChunk)));
    YASSERT(IncompleteChunk.empty());
    CompleteSize = 0;
    IncompleteChunk.reserve(CurrentReserveSize);

    return MoveRV(CompleteChunks);
}

size_t TChunkedOutputStream::GetSize() const
{
    return CompleteSize + IncompleteChunk.size();
}

size_t TChunkedOutputStream::GetCapacity() const
{
    return CompleteSize + IncompleteChunk.capacity();
}

void TChunkedOutputStream::DoWrite(const void* buf, size_t len)
{
    auto writeLen  = std::min(len, IncompleteChunk.capacity() - IncompleteChunk.size());
    auto buffer = static_cast<const ui8*>(buf);
    IncompleteChunk.insert(IncompleteChunk.end(), buffer, buffer + writeLen);

    if (len > writeLen) {
        YASSERT(IncompleteChunk.size() == IncompleteChunk.capacity());
        CompleteSize += IncompleteChunk.size();
        CompleteChunks.push_back(TSharedRef(MoveRV(IncompleteChunk)));
        YASSERT(IncompleteChunk.empty());

        CurrentReserveSize = std::min(2 * CurrentReserveSize, MaxReserveSize);
        IncompleteChunk.reserve(len - writeLen + CurrentReserveSize);
        IncompleteChunk.insert(IncompleteChunk.end(), buffer + writeLen, buffer + len);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT