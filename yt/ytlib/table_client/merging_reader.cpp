#include "stdafx.h"
#include "merging_reader.h"
#include "config.h"
#include "table_chunk_reader.h"

#include <ytlib/chunk_client/key.h>
#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>

#include <core/misc/sync.h>
#include <core/misc/heap.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/yson_string.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

inline bool CompareReaders(
    const TTableChunkSequenceReader* lhs,
    const TTableChunkSequenceReader* rhs)
{
    return CompareKeys(lhs->GetFacade()->GetKey(), rhs->GetFacade()->GetKey()) < 0;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMergingReader
    : public ISyncReader
{
public:
    explicit TMergingReader(const std::vector<TTableChunkSequenceReaderPtr>& readers)
        : Readers(readers)
        , IsStarted_(false)
    { }

    virtual void Open() override
    {
        // Open all readers in parallel and wait until of them are opened.
        auto awaiter = New<TParallelAwaiter>(
            TDispatcher::Get()->GetReaderInvoker());
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

        awaiter->Complete().Get();

        if (!errors.empty()) {
            TError wrappedError("Error opening merging reader");
            wrappedError.InnerErrors() = errors;
            THROW_ERROR wrappedError;
        }

        // Push all non-empty readers to the heap.
        FOREACH (auto reader, Readers) {
            if (reader->GetFacade()) {
                ReaderHeap.push_back(~reader);
            }
        }

        // Prepare the heap.
        if (!ReaderHeap.empty()) {
            MakeHeap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
        }
    }

    virtual const TRow* GetRow() override
    {
        if (IsStarted_) {
            auto* currentReader = ReaderHeap.front();
            if (!currentReader->FetchNext()) {
                Sync(currentReader, &TTableChunkSequenceReader::GetReadyEvent);
            }
            auto* readerFacade = currentReader->GetFacade();
            if (readerFacade) {
                AdjustHeap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
            } else {
                ExtractHeap(ReaderHeap.begin(), ReaderHeap.end(), CompareReaders);
                ReaderHeap.pop_back();
            }
        }
        IsStarted_ = true;

        if (ReaderHeap.empty()) {
            return nullptr;
        } else {
            return &(ReaderHeap.front()->GetFacade()->GetRow());
        }
    }

    virtual const TNonOwningKey& GetKey() const override
    {
        return ReaderHeap.front()->GetFacade()->GetKey();
    }

    virtual i64 GetSessionRowCount() const override
    {
        i64 total = 0;
        FOREACH (const auto& reader, Readers) {
            total += reader->GetProvider()->GetRowCount();
        }
        return total;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics = NChunkClient::NProto::ZeroDataStatistics();

        FOREACH (const auto& reader, Readers) {
            dataStatistics += reader->GetProvider()->GetDataStatistics();
        }
        return dataStatistics;   
    }

    virtual const TNullable<int>& GetTableIndex() const override
    {
        return ReaderHeap.front()->GetFacade()->GetTableIndex();
    }

    virtual i64 GetSessionRowIndex() const override
    {
        i64 total = 0;
        FOREACH (const auto& reader, Readers) {
            total += reader->GetProvider()->GetRowIndex();
        }
        return total;
    }
    
    virtual i64 GetTableRowIndex() const override
    {
        YUNREACHABLE();
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        std::vector<TChunkId> result;
        FOREACH (auto reader, Readers) {
            auto part = reader->GetFailedChunkIds();
            result.insert(result.end(), part.begin(), part.end());
        }
        return result;
    }

private:
    std::vector<TTableChunkSequenceReaderPtr> Readers;
    std::vector<TTableChunkSequenceReader*> ReaderHeap;

    bool IsStarted_;
};

ISyncReaderPtr CreateMergingReader(const std::vector<TTableChunkSequenceReaderPtr>& readers)
{
    return New<TMergingReader>(readers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
