#include "stdafx.h"
#include "merging_reader.h"
#include "table_chunk_sequence_reader.h"
#include "key.h"

#include <ytlib/misc/sync.h>
#include <ytlib/misc/heap.h>

#include <ytlib/actions/parallel_awaiter.h>

#include <ytlib/ytree/yson_string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

inline bool CompareReaders(
    const TTableChunkSequenceReader* lhs,
    const TTableChunkSequenceReader* rhs)
{
    return CompareKeys(lhs->GetKey(), rhs->GetKey()) < 0;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMergingReader
    : public ISyncReader
{
public:
    explicit TMergingReader(const std::vector<TTableChunkSequenceReaderPtr>& readers)
        : Readers(readers)
    { }

    virtual void Open() override
    {
        // Open all readers in parallel and wait until of them are opened.
        auto awaiter = New<TParallelAwaiter>(NChunkClient::ReaderThread->GetInvoker());
        std::vector<TError> errors;

        FOREACH (auto reader, Readers) {
            awaiter->Await(
                reader->AsyncOpen(),
                BIND([&] (TError error) {
                    if (!error.IsOK()) {
                        errors.push_back(error);
                    }
            }));
        }

        TPromise<void> completed(NewPromise<void>());
        awaiter->Complete(BIND([=] () mutable {
            completed.Set();
        }));
        completed.Get();

        if (!errors.empty()) {
            Stroka message = "Error opening merging reader\n";
            FOREACH (const auto& error, errors) {
                message.append("\n");
                message.append(error.ToString());
            }
            ythrow yexception() << message;
        }

        // Push all non-empty readers to the heap.
        FOREACH (auto reader, Readers) {
            if (reader->IsValid()) {
                ReaderHeap.push_back(~reader);
            }
        }

        // Prepare the heap.
        if (!ReaderHeap.empty()) {
            MakeHeap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
        }
    }

    virtual void NextRow() override
    {
        if (ReaderHeap.empty())
            return;

        auto* currentReader = ReaderHeap.front();

        if (!currentReader->FetchNextItem()) {
            Sync(currentReader, &TTableChunkSequenceReader::GetReadyEvent);
        }

        if (currentReader->IsValid()) {
            AdjustHeap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
        } else {
            ExtractHeap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
            ReaderHeap.pop_back();
        }
    }

    virtual bool IsValid() const override
    {
        return !ReaderHeap.empty();
    }

    virtual const TRow& GetRow() const override
    {
        return ReaderHeap.front()->GetRow();
    }

    virtual const TNonOwningKey& GetKey() const override
    {
        return ReaderHeap.front()->GetKey();
    }

private:
    std::vector<TTableChunkSequenceReaderPtr> Readers;
    std::vector<TTableChunkSequenceReader*> ReaderHeap;

};

ISyncReaderPtr CreateMergingReader(const std::vector<TTableChunkSequenceReaderPtr>& readers)
{
    return New<TMergingReader>(readers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
