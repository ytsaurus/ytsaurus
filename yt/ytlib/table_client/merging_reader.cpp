#include "stdafx.h"
#include "merging_reader.h"
#include "table_chunk_sequence_reader.h"
#include "key.h"

#include <ytlib/misc/sync.h>
#include <ytlib/ytree/yson_string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

    inline bool CompareReaders(
        const TTableChunkSequenceReader* lhs,
        const TTableChunkSequenceReader* rhs)
    {
        return CompareKeys(lhs->GetKey(), rhs->GetKey()) > 0;
    }

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMergingReader::TMergingReader(const std::vector<TTableChunkSequenceReaderPtr>& readers)
    : Readers(readers)
{ }

void TMergingReader::Open()
{
    FOREACH (auto reader, Readers) {
        Sync(~reader, &TTableChunkSequenceReader::AsyncOpen);
        if (reader->IsValid()) {
            ReaderHeap.push_back(~reader);
        }
    }

    std::make_heap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);

    if (!ReaderHeap.empty()) {
        std::pop_heap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
    }
}

void TMergingReader::NextRow()
{
    if (ReaderHeap.empty())
        return;

    if (!ReaderHeap.back()->FetchNextItem()) {
        Sync(ReaderHeap.back(), &TTableChunkSequenceReader::GetReadyEvent);
    }

    if (ReaderHeap.back()->IsValid()) {
        std::push_heap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
        std::pop_heap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
    } else {
        ReaderHeap.pop_back();
    }
}

bool TMergingReader::IsValid() const
{
    return !ReaderHeap.empty();
}

const TRow& TMergingReader::GetRow()
{
    return ReaderHeap.back()->GetRow();
}

const TNonOwningKey& TMergingReader::GetKey() const
{
    return ReaderHeap.back()->GetKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
