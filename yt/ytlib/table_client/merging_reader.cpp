#include "stdafx.h"
#include "merging_reader.h"
#include "chunk_sequence_reader.h"
#include "key.h"

#include <ytlib/misc/sync.h>
#include <ytlib/ytree/yson_string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

    inline bool CompareReaders(
        const TChunkSequenceReader* lhs,
        const TChunkSequenceReader* rhs)
    {
        return CompareKeys(lhs->GetKey(), rhs->GetKey()) > 0;
    }

} // namespace


TMergingReader::TMergingReader(const std::vector<TChunkSequenceReaderPtr>& readers)
    : Readers(readers)
{ }

void TMergingReader::Open()
{
    FOREACH (auto reader, Readers) {
        Sync(~reader, &TChunkSequenceReader::AsyncOpen);
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

    Sync(ReaderHeap.back(), &TChunkSequenceReader::AsyncNextRow);
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

TRow& TMergingReader::GetRow()
{
    return ReaderHeap.back()->GetRow();
}

const NYTree::TYsonString& TMergingReader::GetRowAttributes() const
{
    throw ReaderHeap.back()->GetRowAttributes();
}

const TNonOwningKey& TMergingReader::GetKey() const
{
    return ReaderHeap.back()->GetKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
