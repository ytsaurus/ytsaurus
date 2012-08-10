#include "stdafx.h"
#include "sorting_reader.h"
#include "config.h"
#include "private.h"
#include "small_key.h"

#include <ytlib/misc/heap.h>

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
static const int SpinsBetweenYield = 1000;
static const int RowsBetweenAtomicUpdate = 10000;
static const i32 BucketEndSentinel = -1;

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

    int EstimatedRowCount;
    int EstimatedBucketCount;

    int TotalRowCount;
    TAtomic SortedRowCount;
    int ReadRowCount;

    TMemoryInput RowInput;
    TRow CurrentRow;
    TNonOwningKey CurrentKey;

    std::vector<TSmallKeyPart> KeyBuffer;
    std::vector<const char*> RowPtrBuffer;
    std::vector<i32> Buckets;
    std::vector<i32> SortedIndexes;
    std::vector<int> BucketStart;
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

        bool CompareRows(i32 lhs, i32 rhs) const
        {
            int lhsStartIndex = lhs * KeyColumnCount;
            int lhsEndIndex   = lhsStartIndex + KeyColumnCount;
            int rhsStartIndex = rhs * KeyColumnCount;
            for (int lhsIndex = lhsStartIndex, rhsIndex = rhsStartIndex;
                lhsIndex < lhsEndIndex;
                ++lhsIndex, ++rhsIndex)
            {
                auto res = CompareSmallKeyParts(KeyBuffer[lhsIndex], KeyBuffer[rhsIndex]);
                if (res < 0)
                    return true;
                if (res > 0)
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
        bool operator () (i32 lhs, i32 rhs) const
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
        { }

        // Returns True iff row[Buckets[lhs]] < row[Buckets[rhs]]
        bool operator () (int lhs, int rhs) const
        {
            return CompareRows(Buckets[lhs], Buckets[rhs]);
        };

    private:
        std::vector<i32>& Buckets;

    };

    TSortComparer SortComparer;
    TMergeComparer MergeComparer;


    void InitInput()
    {
        LOG_INFO("Initializing input");
        PROFILE_TIMING ("/reduce/init_time") {
            Sync(~Reader, &TPartitionChunkSequenceReader::AsyncOpen);

            EstimatedRowCount = Reader->GetRowCount();
            EstimatedBucketCount = (EstimatedRowCount + SortBucketSize - 1) / SortBucketSize;
            LOG_INFO("Input size estimated (RowCount: %d, BucketCount: %d)",
                EstimatedRowCount,
                EstimatedBucketCount);
            YCHECK(EstimatedRowCount <= std::numeric_limits<i32>::max());

            KeyBuffer.reserve(EstimatedRowCount * KeyColumnCount);
            RowPtrBuffer.reserve(EstimatedRowCount);
            Buckets.reserve(EstimatedRowCount + EstimatedBucketCount);
            SortedIndexes.reserve(EstimatedRowCount);
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
                Buckets.push_back(BucketEndSentinel);
                BucketStart.push_back(Buckets.size());
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
            int bucketCount = static_cast<int>(BucketStart.size()) - 1;

            YCHECK(TotalRowCount <= EstimatedRowCount);
            YCHECK(bucketCount <= EstimatedBucketCount);

            LOG_INFO("Finished reading input (RowCount: %d, BucketCount: %d)",
                TotalRowCount,
                bucketCount);
        }
    }

    void DoSortBucket(int bucketId)
    {
        LOG_DEBUG("Started sorting bucket %d", bucketId);

        int startIndex = BucketStart[bucketId];
        int endIndex = BucketStart[bucketId + 1] - 1;
        std::sort(Buckets.begin() + startIndex, Buckets.begin() + endIndex, SortComparer);

        LOG_DEBUG("Finished sorting bucket %d", bucketId);
    }

    void StartMerge()
    {
        LOG_INFO("Waiting for sort thread");
        PROFILE_TIMING ("/reduce/sort_wait_time") {
            BIND([] () -> TVoid { return TVoid(); }).AsyncVia(SortQueue->GetInvoker()).Run().Get();
        }
        LOG_INFO("Sort thread is idle");

        for (int index = 0; index < static_cast<int>(BucketStart.size()) - 1; ++index) {
            BucketHeap.push_back(BucketStart[index]);
        }

        MakeHeap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);

        AtomicSet(SortedRowCount, 0);
        ReadRowCount = 0;

        SortQueue->GetInvoker()->Invoke(BIND(&TSortingReader::DoMerge, Unretained(this)));
    }

    void DoMerge()
    {
        LOG_INFO("Started merge");
        PROFILE_TIMING ("/reduce/merge_time") {
            int sortedRowCount = 0;
            while (!BucketHeap.empty()) {
                int bucketIndex = BucketHeap.front();
                SortedIndexes.push_back(Buckets[bucketIndex]);
                ++bucketIndex;
                if (Buckets[bucketIndex] == BucketEndSentinel) {
                    ExtractHeap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
                    BucketHeap.pop_back();
                } else {
                    BucketHeap.front() = bucketIndex;
                    AdjustHeap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
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

