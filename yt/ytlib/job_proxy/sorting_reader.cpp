#include "stdafx.h"
#include "sorting_reader.h"
#include "config.h"
#include "private.h"
#include "small_key.h"

#include <ytlib/actions/action_queue.h>

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/partition_chunk_sequence_reader.h>

#include <ytlib/rpc/channel.h>

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/ytree/lexer.h>

#include <util/system/yield.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NYTree;
 
////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

class TSortingReader
    : public ISyncReader
{
public:
    TSortingReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        TClosure onNetworkReleased,
        std::vector<NTableClient::NProto::TInputChunk>&& chunks)
        : KeyColumns(keyColumns)
        , KeyColumnCount(static_cast<int>(KeyColumns.size()))
        , OnNetworkReleased(onNetworkReleased)
        , OpenError(NewPromise<TError>())
        , IsValid_(true)
        , CurrentKey(KeyColumnCount)
    {
        srand(time(NULL));
        std::random_shuffle(chunks.begin(), chunks.end());

        Reader = New<TPartitionChunkSequenceReader>(
            config,
            masterChannel, 
            blockCache, 
            MoveRV(chunks));
    }

    virtual void Open() override
    {
        SortQueue = New<TActionQueue>("Sort");
        SortQueue->GetInvoker()->Invoke(BIND(&TSortingReader::SortThreadMain, MakeStrong(this)));

        LOG_INFO("Waiting for sort thread to finish reading");

        auto error = OpenError.Get();
        if (!error.IsOK()) {
            SetInvalid();
            ythrow yexception() << error.ToString();
        }

        LOG_INFO("Sort thread has finished reading");

        if (TotalRowCount == 0) {
            SetInvalid();
        } else {
            DoNextRow();
        }
    }

    virtual bool IsValid() const override
    {
        return IsValid_;
    }

    virtual const TRow& GetRow() const override
    {
        return CurrentRow;
    }

    virtual const TNonOwningKey& GetKey() const override
    {
        return CurrentKey;
    }

    virtual void NextRow() override
    {
        YASSERT(IsValid_);
        DoNextRow();
    }

private:
    TKeyColumns KeyColumns;
    int KeyColumnCount;
    TClosure OnNetworkReleased;

    bool IsValid_;
    TPartitionChunkSequenceReaderPtr Reader;
    TActionQueuePtr SortQueue;
    TAsyncErrorPromise OpenError;

    TAtomic TotalRowCount;
    TAtomic SortedRowIndex;
    TAtomic CurrentRowIndex;

    TMemoryInput RowInput;
    TRow CurrentRow;
    TNonOwningKey CurrentKey;

    std::vector<TSmallKeyPart> KeyBuffer;
    std::vector<const char*> RowPtrBuffer;
    std::vector<ui32> RowIndexHeap;

    void SortThreadMain()
    {
        PROFILE_TIMING ("/sort_time") {
            // comparer(x, y) returns True iff row[x] < row[y]
            auto comparer = [&] (ui32 lhs, ui32 rhs) -> bool {
                int lhsStartIndex = lhs * KeyColumnCount;
                int lhsEndIndex   = lhsStartIndex + KeyColumnCount;
                int rhsStartIndex = rhs * KeyColumnCount;
                for (int lhsIndex = lhsStartIndex, rhsIndex = rhsStartIndex;
                    lhsIndex < lhsEndIndex;
                    ++lhsIndex, ++rhsIndex)
                {
                    auto res = CompareSmallKeyParts(KeyBuffer[lhsIndex], KeyBuffer[rhsIndex]);
                    if (res > 0)
                        return true;
                    if (res < 0)
                        return false;
                }
                return false;
            };

            try {
                LOG_INFO("Initializing input");
                {
                    Sync(~Reader, &TPartitionChunkSequenceReader::AsyncOpen);

                    i64 estimatedRowCount = Reader->GetRowCount();
                    LOG_INFO("Estimated row count: %" PRId64, estimatedRowCount);

                    KeyBuffer.reserve(estimatedRowCount * KeyColumnCount);
                    RowPtrBuffer.reserve(estimatedRowCount);
                    RowIndexHeap.reserve(estimatedRowCount);
                }
                PROFILE_TIMING_CHECKPOINT("init");

                LOG_INFO("Reading sort input");
                {
                    bool isNetworkReleased = false;

                    TLexer lexer;
                    while (Reader->IsValid()) {
                        // Push row pointer and heap entry.
                        RowPtrBuffer.push_back(Reader->CurrentReader()->GetRowPointer());
                        RowIndexHeap.push_back(RowIndexHeap.size());
                        YASSERT(RowIndexHeap.back() <= std::numeric_limits<ui32>::max());

                        // Push key.
                        KeyBuffer.resize(KeyBuffer.size() + KeyColumnCount);
                        for (int i = 0; i < KeyColumnCount; ++i) {
                            auto value = Reader->CurrentReader()->ReadValue(KeyColumns[i]);
                            if (!value.IsNull()) {
                                auto& keyPart = KeyBuffer[RowIndexHeap.back() * KeyColumnCount + i];
                                SetSmallKeyPart(keyPart, value.ToStringBuf(), lexer);
                            }
                        }

                        // Readjust the heap.
                        std::push_heap(RowIndexHeap.begin(), RowIndexHeap.end(), comparer);

                        if (!isNetworkReleased && Reader->IsFetchingComplete()) {
                            OnNetworkReleased.Run();
                            isNetworkReleased =  true;
                        }

                        if (!Reader->FetchNextItem()) {
                            Sync(~Reader, &TPartitionChunkSequenceReader::GetReadyEvent);
                        }
                    }

                    if (!isNetworkReleased) {
                        OnNetworkReleased.Run();
                        isNetworkReleased =  true;
                    }
                }
                PROFILE_TIMING_CHECKPOINT("read");
            } catch (const std::exception& ex) {
                OpenError.Set(TError(ex.what()));
            }

            AtomicSet(TotalRowCount, RowIndexHeap.size());
            AtomicSet(SortedRowIndex, TotalRowCount);
            AtomicSet(CurrentRowIndex, TotalRowCount);
            
            LOG_INFO("Total row count: %" PRISZT, TotalRowCount);

            OpenError.Set(TError());

            LOG_INFO("Sorting input");
            {
                auto heapBegin = RowIndexHeap.begin();
                auto heapEnd = RowIndexHeap.end();
                
                // Pop heap until empty. Notify the client periodically.
                const int RowsBetweenAtomicUpdate = 100;
                size_t sortedRowIndex = TotalRowCount;
                while (heapBegin != heapEnd) {
                    std::pop_heap(heapBegin, heapEnd, comparer);
                    --heapEnd;
                    --sortedRowIndex;
                    if (sortedRowIndex % RowsBetweenAtomicUpdate) {
                        AtomicSet(SortedRowIndex, sortedRowIndex);
                    }
                }

                AtomicSet(SortedRowIndex, 0);
            }
            PROFILE_TIMING_CHECKPOINT("sort");

            LOG_INFO("Sorting complete");
        }
    }

    void DoNextRow()
    {
        if (CurrentRowIndex == 0) {
            SetInvalid();
            return;
        }

        auto currentRowIndex = AtomicDecrement(CurrentRowIndex);
        const int SpinsBetweenYield = 1000;
        for (int spinCounter = 1; ; ++spinCounter) {
            auto sortedRowIndex = AtomicGet(SortedRowIndex);
            if (sortedRowIndex <= currentRowIndex) {
                break;
            }
            if (spinCounter % SpinsBetweenYield == 0) {
                ThreadYield();
            } else {
                SpinLockPause();
            }
        }

        auto sortedRowIndex = RowIndexHeap[currentRowIndex];

        // Prepare key.
        CurrentKey.Clear();
        for (int index = 0; index < KeyColumnCount; ++index) {
            const auto& keyPart = KeyBuffer[sortedRowIndex * KeyColumnCount + index];
            SetKeyPart(&CurrentKey, keyPart, index);
        }

        // Prepare row.
        CurrentRow.clear();
        RowInput.Reset(RowPtrBuffer[sortedRowIndex], std::numeric_limits<size_t>::max());
        while (true) {
            auto value = TValue::Load(&RowInput);
            if (value.IsNull()) {
                break;
            }

            i32 columnNameLength;
            ReadVarInt32(&RowInput, &columnNameLength);
            YASSERT(columnNameLength > 0);
            CurrentRow.push_back(std::make_pair(
                TStringBuf(RowInput.Buf(), columnNameLength),
                value.ToStringBuf()));
            RowInput.Skip(columnNameLength);
        }

    }

    void SetInvalid()
    {
        YCHECK(IsValid_);
        IsValid_ = false;
        SortQueue->Shutdown();
    }

};

ISyncReaderPtr CreateSortingReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    TClosure onNetworkReleased,
    std::vector<NTableClient::NProto::TInputChunk>&& chunks)
{
    return New<TSortingReader>(
        config,
        masterChannel,
        blockCache,
        keyColumns,
        onNetworkReleased,
        MoveRV(chunks));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

