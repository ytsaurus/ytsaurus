#include "stdafx.h"

#include "chunked_output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChunkedOutputStream::TChunkedOutputStream(int minChunkSize)
    : MinChunkSize(minChunkSize)
    , CompleteSize(0)
{ }

TChunkedOutputStream::~TChunkedOutputStream() throw()
{ }

std::vector<TSharedRef> TChunkedOutputStream::FlushBuffer()
{
    CompleteChunks.push_back(TSharedRef(MoveRV(IncompleteChunk)));
    YASSERT(IncompleteChunk.empty());
    return MoveRV(CompleteChunks);
}

size_t TChunkedOutputStream::GetSize() const
{
    return CompleteSize + IncompleteChunk.size();
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
        IncompleteChunk.reserve(len - writeLen + MinChunkSize);
        IncompleteChunk.insert(IncompleteChunk.end(), buffer + writeLen, buffer + len);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT