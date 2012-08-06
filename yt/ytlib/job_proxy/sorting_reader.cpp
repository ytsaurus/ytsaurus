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

static const int SortBucketSize = 10000;
const int SpinsBetweenYield = 1000;
static const int RowsBetweenAtomicUpdate = 1000;

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
        , IsValid_(true)
        , CurrentKey(KeyColumnCount)
        , SortComparer(this)
        , MergeComparer(this)
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

        InitInput();
        ReadInput();
        StartMerge();
        DoNextRow();
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
        //YASSERT(IsValid_);
        //DoNextRow();
    }

private:
    TKeyColumns KeyColumns;
    int KeyColumnCount;
    TClosure OnNetworkReleased;

    bool IsValid_;
    TPartitionChunkSequenceReaderPtr Reader;
    TActionQueuePtr SortQueue;

    int TotalRowCount;
    TAtomic SortedRowCount;
    int ReadRowCount;

    TMemoryInput RowInput;
    TRow CurrentRow;
    TNonOwningKey CurrentKey;

    std::vector<TSmallKeyPart> KeyBuffer;
    std::vector<const char*> RowPtrBuffer;
    std::vector<ui32> Buckets;
    std::vector<ui32> SortedIndexes;
    std::vector<int> BucketStart;
    std::vector<int> BucketCurrent;
    std::vector<int> BucketHeap;

    class TComparerBase
    {
    public:
        explicit TComparerBase(TSortingReader* reader)
            : KeyColumnCount(reader->KeyColumnCount)
            , KeyBuffer(reader->KeyBuffer)
        { }

    protected:
        int KeyColumnCount;
        std::vector<TSmallKeyPart>& KeyBuffer;

        bool CompareRows(ui32 lhs, ui32 rhs) const
        {
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
        }
    };

    class TSortComparer
        : public TComparerBase
    {
    public:
        explicit TSortComparer(TSortingReader* reader)
            : TComparerBase(reader)
        { }

        // Returns True iff row[lhs] < row[rhs]
        bool operator () (ui32 lhs, ui32 rhs) const
        {
            return CompareRows(lhs, rhs);
        };

    };

    class TMergeComparer
        : public TComparerBase
    {
    public:
        explicit TMergeComparer(TSortingReader* reader)
            : TComparerBase(reader)
            , Buckets(reader->Buckets)
            , BucketCurrent(reader->BucketCurrent)
        { }

        // Returns True iff row[Buckets[BucketCurrent[lhs]]] < row[Buckets[BucketCurrent[rhs]]]
        bool operator () (int lhs, int rhs) const
        {
            return CompareRows(Buckets[BucketCurrent[lhs]], Buckets[BucketCurrent[rhs]]);
        };

    private:
        std::vector<ui32>& Buckets;
        std::vector<int>& BucketCurrent;

    };

    TSortComparer SortComparer;
    TMergeComparer MergeComparer;


    void InitInput()
    {
        LOG_INFO("Initializing input");
        PROFILE_TIMING ("/reduce/init_time") {
            Sync(~Reader, &TPartitionChunkSequenceReader::AsyncOpen);

            i64 estimatedRowCount = Reader->GetRowCount();
            LOG_INFO("Input size estimated (RowCount: %" PRId64 ")", estimatedRowCount);
            YCHECK(estimatedRowCount <= std::numeric_limits<i32>::max());

            KeyBuffer.reserve(estimatedRowCount * KeyColumnCount);
            RowPtrBuffer.reserve(estimatedRowCount);
            Buckets.reserve(estimatedRowCount);
            SortedIndexes.reserve(estimatedRowCount);
        }
    }

    void ReadInput()
    {
        LOG_INFO("Started reading input");
        PROFILE_TIMING ("/reduce/read_time" ) {
            bool isNetworkReleased = false;

            TLexer lexer;
            int bucketId = 0;
            int bucketSize = 0;
            int rowIndex = 0;

            auto flushBucket = [&] () {
                BucketStart.push_back(rowIndex);
                SortQueue->GetInvoker()->Invoke(BIND(&TSortingReader::DoSortBucket, Unretained(this), bucketId));
                ++bucketId;
                bucketSize = 0;
            };

            BucketStart.push_back(0);

            while (Reader->IsValid()) {
                // Construct row entry.
                RowPtrBuffer.push_back(Reader->CurrentReader()->GetRowPointer());

                // Construct key entry.
                KeyBuffer.resize(KeyBuffer.size() + KeyColumnCount);
                for (int i = 0; i < KeyColumnCount; ++i) {
                    auto value = Reader->CurrentReader()->ReadValue(KeyColumns[i]);
                    if (!value.IsNull()) {
                        auto& keyPart = KeyBuffer[rowIndex * KeyColumnCount + i];
                        SetSmallKeyPart(keyPart, value.ToStringBuf(), lexer);
                    }
                }

                // Push the row to the current bucket and flush the bucket if full.
                Buckets.push_back(rowIndex);
                ++rowIndex;
                ++bucketSize;
                if (bucketSize == SortBucketSize) {
                    flushBucket();
                }

                if (!isNetworkReleased && Reader->IsFetchingComplete()) {
                    OnNetworkReleased.Run();
                    isNetworkReleased =  true;
                }

                if (!Reader->FetchNextItem()) {
                    Sync(~Reader, &TPartitionChunkSequenceReader::GetReadyEvent);
                }
            }

            if (bucketSize > 0) {
                flushBucket();
            }

            if (!isNetworkReleased) {
                OnNetworkReleased.Run();
                isNetworkReleased =  true;
            }

            TotalRowCount = rowIndex;

            LOG_INFO("Finished reading input (TotalRowCount: %d, BucketCount: %d)",
                TotalRowCount,
                static_cast<int>(BucketStart.size()) - 1);
        }
    }

    void DoSortBucket(int bucketId)
    {
        LOG_DEBUG("Starting sorting bucket %d: rows %d-%d",
            bucketId,
            BucketStart[bucketId],
            BucketStart[bucketId + 1]);

        auto begin = Buckets.begin() + BucketStart[bucketId];
        auto end = Buckets.end() + BucketStart[bucketId + 1];
        std::sort(begin, end, SortComparer);

        LOG_DEBUG("Finished sorting bucket %d", bucketId);
    }

    void StartMerge()
    {
        BucketCurrent.reserve(BucketStart.size());
        for (int index = 0; index < static_cast<int>(BucketStart.size()) - 1; ++index) {
            BucketHeap.push_back(index);
            BucketCurrent.push_back(BucketStart[index]);
        }

        std::make_heap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
        std::pop_heap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);

        AtomicSet(SortedRowCount, 0);
        ReadRowCount = 0;

        LOG_INFO("Waiting for sort thread");
        PROFILE_TIMING ("/reduce/sort_wait_time") {
            BIND([] () -> TVoid { return TVoid(); }).AsyncVia(SortQueue->GetInvoker()).Run().Get();
        }
        LOG_INFO("Sort thread is idle");

        SortQueue->GetInvoker()->Invoke(BIND(&TSortingReader::DoMerge, Unretained(this)));
    }

    void DoMerge()
    {
        LOG_INFO("Starting merge");
        PROFILE_TIMING ("/reduce/merge_time") {
            int sortedRowCount = 0;
            while (!BucketHeap.empty()) {
                int bucketId = BucketHeap.back();
                int current = BucketCurrent[bucketId];
                int end = BucketStart[bucketId + 1];
                SortedIndexes.push_back(Buckets[current]);
                if (current == end) {
                    BucketHeap.pop_back();
                } else {
                    BucketCurrent[bucketId] = current + 1;
                    std::push_heap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
                    std::pop_heap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
                }

                ++sortedRowCount;
                if (sortedRowCount % RowsBetweenAtomicUpdate == 0) {
                    AtomicSet(SortedRowCount, sortedRowCount);
                }
            }

            YCHECK(sortedRowCount == TotalRowCount);
            AtomicSet(SortedRowCount, sortedRowCount);
        }
        LOG_INFO("Finished merge");
    }

    void DoNextRow()
    {
        if (ReadRowCount == TotalRowCount) {
            SetInvalid();
            return;
        }

        int currentIndex = ReadRowCount;
        for (int spinCounter = 1; ; ++spinCounter) {
            auto sortedRowCount = spinCounter == 1 ? SortedRowCount : AtomicGet(SortedRowCount);
            if (sortedRowCount > currentIndex) {
                break;
            }
            if (spinCounter % SpinsBetweenYield == 0) {
                ThreadYield();
            } else {
                SpinLockPause();
            }
        }

        auto sortedIndex = SortedIndexes[currentIndex];

        // Prepare key.
        CurrentKey.Clear();
        for (int index = 0; index < KeyColumnCount; ++index) {
            const auto& keyPart = KeyBuffer[sortedIndex * KeyColumnCount + index];
            SetKeyPart(&CurrentKey, keyPart, index);
        }

        // Prepare row.
        CurrentRow.clear();
        RowInput.Reset(RowPtrBuffer[sortedIndex], std::numeric_limits<size_t>::max());
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

        ++ReadRowCount;
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

