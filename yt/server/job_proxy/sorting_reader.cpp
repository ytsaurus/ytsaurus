#include "stdafx.h"
#include "sorting_reader.h"
#include "config.h"
#include "private.h"
#include "small_key.h"

#include <core/misc/heap.h>
#include <core/misc/varint.h>

#include <core/concurrency/action_queue.h>

#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/partition_chunk_reader.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/rpc/channel.h>

#include <ytlib/chunk_client/block_cache.h>

#include <core/yson/lexer.h>

#include <util/system/yield.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NVersionedTableClient;

using NTableClient::TRow;
using NVersionedTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

static const int SortBucketSize = 10000;
static const int SpinsBetweenYield = 1000;
static const int RowsBetweenAtomicUpdate = 10000;
static const i32 BucketEndSentinel = -1;
static const double ReallocationFactor = 1.1;

////////////////////////////////////////////////////////////////////////////////

struct TKeyMemoryPoolTag { };

class TSortingReader
    : public ISyncReader
{
public:
    TSortingReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        IBlockCachePtr compressedBlockCache,
        IBlockCachePtr uncompressedBlockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const TKeyColumns& keyColumns,
        TClosure onNetworkReleased,
        std::vector<NChunkClient::NProto::TChunkSpec>&& chunks,
        int estimatedRowCount,
        bool isApproximate)
        : KeyColumns(keyColumns)
        , KeyColumnCount(static_cast<int>(KeyColumns.size()))
        , OnNetworkReleased(onNetworkReleased)
        , IsValid_(true)
        , IsApproximate(isApproximate)
        , EstimatedRowCount(estimatedRowCount)
        , TotalRowCount(0)
        , ReadRowCount(0)
        , KeyMemoryPool(TKeyMemoryPoolTag())
        , CurrentKey(TKey::Allocate(&KeyMemoryPool, KeyColumnCount))
        , SortComparer(this)
        , MergeComparer(this)
    {
        srand(time(nullptr));
        std::random_shuffle(chunks.begin(), chunks.end());

        auto provider = New<TPartitionChunkReaderProvider>(
            config,
            uncompressedBlockCache);

        Reader = New<TReader>(
            config,
            masterChannel,
            compressedBlockCache,
            nodeDirectory,
            std::move(chunks),
            provider);
    }

    virtual void Open() override
    {
        SortQueue = New<TActionQueue>("Sort");

        InitInput();
        ReadInput();
        StartMerge();
    }

    virtual const TRow* GetRow() override
    {
        DoNextRow();
        return IsValid_ ? &CurrentRow : nullptr;
    }

    virtual const TKey& GetKey() const override
    {
        return CurrentKey;
    }

    virtual i64 GetSessionRowCount() const override
    {
        return TotalRowCount;
    }

    virtual i64 GetTableRowIndex() const override
    {
        YUNREACHABLE();
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return ReadRowCount;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Reader->GetProvider()->GetDataStatistics();
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return Reader->GetFailedChunkIds();
    }

    virtual int GetTableIndex() const override
    {
        // When reading from partition chunk no row attributes are preserved.
        return 0;
    }

private:
    typedef TOldMultiChunkParallelReader<TPartitionChunkReader> TReader;
    typedef TIntrusivePtr<TReader> TReaderPtr;

    TKeyColumns KeyColumns;
    int KeyColumnCount;
    TClosure OnNetworkReleased;

    bool IsValid_;
    TReaderPtr Reader;
    TActionQueuePtr SortQueue;

    bool IsApproximate;

    int EstimatedRowCount;
    int EstimatedBucketCount;

    int TotalRowCount;
    TAtomic SortedRowCount;
    int ReadRowCount;

    TMemoryInput RowInput;
    TRow CurrentRow;

    TChunkedMemoryPool KeyMemoryPool;
    TKey CurrentKey;

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
            Sync(Reader.Get(), &TReader::AsyncOpen);

            EstimatedBucketCount = (EstimatedRowCount + SortBucketSize - 1) / SortBucketSize;
            LOG_INFO("Input size estimated (RowCount: %v, BucketCount: %v)",
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

            TStatelessLexer lexer;
            int bucketId = 0;
            int bucketSize = 0;
            int rowIndex = 0;

            auto flushBucket = [&] () {
                SafePushBack(Buckets, BucketEndSentinel);
                SafePushBack(BucketStart, Buckets.size());
                InvokeSortBucket(bucketId);
                ++bucketId;
                bucketSize = 0;
            };

            SafePushBack(BucketStart, 0);

            const TReader::TFacade* facade;
            while ((facade = Reader->GetFacade()) != nullptr) {
                // Construct row entry.
                SafePushBack(RowPtrBuffer, facade->GetRowPointer());

                // Construct key entry.
                KeyBuffer.resize(KeyBuffer.size() + KeyColumnCount);
                for (int i = 0; i < KeyColumnCount; ++i) {
                    auto value = facade->ReadValue(KeyColumns[i]);
                    if (!value.IsNull()) {
                        auto& keyPart = KeyBuffer[rowIndex * KeyColumnCount + i];
                        SetSmallKeyPart(keyPart, value.ToStringBuf(), lexer);
                    }
                }

                // Push the row to the current bucket and flush the bucket if full.
                SafePushBack(Buckets, rowIndex);
                ++rowIndex;
                ++bucketSize;
                if (bucketSize == SortBucketSize) {
                    flushBucket();
                }

                if (!isNetworkReleased && Reader->GetIsFetchingComplete()) {
                    OnNetworkReleased.Run();
                    isNetworkReleased =  true;
                }

                if (!Reader->FetchNext()) {
                    Sync(Reader.Get(), &TReader::GetReadyEvent);
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

            if (!IsApproximate) {
                YCHECK(TotalRowCount <= EstimatedRowCount);
                YCHECK(bucketCount <= EstimatedBucketCount);
            }

            LOG_INFO("Finished reading input (RowCount: %v, BucketCount: %v)",
                TotalRowCount,
                bucketCount);
        }
    }

    void DoSortBucket(int bucketId)
    {
        LOG_DEBUG("Started sorting bucket %v", bucketId);

        int startIndex = BucketStart[bucketId];
        int endIndex = BucketStart[bucketId + 1] - 1;
        std::sort(Buckets.begin() + startIndex, Buckets.begin() + endIndex, SortComparer);

        LOG_DEBUG("Finished sorting bucket %v", bucketId);
    }

    void StartMerge()
    {
        LOG_INFO("Waiting for sort thread");
        PROFILE_TIMING ("/reduce/sort_wait_time") {
            SortQueueBarrier();
        }
        LOG_INFO("Sort thread is idle");

        for (int index = 0; index < static_cast<int>(BucketStart.size()) - 1; ++index) {
            BucketHeap.push_back(BucketStart[index]);
        }

        MakeHeap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);

        AtomicSet(SortedRowCount, 0);
        ReadRowCount = 0;

        InvokeMerge();
    }

    void DoMerge()
    {
        LOG_INFO("Started merge");
        PROFILE_TIMING ("/reduce/merge_time") {
            int sortedRowCount = 0;
            while (!BucketHeap.empty()) {
                int bucketIndex = BucketHeap.front();
                if (SortedIndexes.size() > 0) {
                    YASSERT(!SortComparer(Buckets[bucketIndex], SortedIndexes.back()));
                }
                SortedIndexes.push_back(Buckets[bucketIndex]);
                ++bucketIndex;
                if (Buckets[bucketIndex] == BucketEndSentinel) {
                    ExtractHeap(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
                    BucketHeap.pop_back();
                } else {
                    BucketHeap.front() = bucketIndex;
                    AdjustHeapFront(BucketHeap.begin(), BucketHeap.end(), MergeComparer);
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

    void ClearKey()
    {
        for (int i = 0; i < KeyColumnCount; ++i) {
            CurrentKey[i] = MakeUnversionedSentinelValue(EValueType::Null);
        }
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
        ClearKey();
        for (int index = 0; index < KeyColumnCount; ++index) {
            const auto& keyPart = KeyBuffer[sortedIndex * KeyColumnCount + index];
            CurrentKey[index] = MakeKeyPart(keyPart);
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


    void SortQueueBarrier()
    {
        BIND([] () { }).AsyncVia(SortQueue->GetInvoker()).Run().Get();
    }

    void InvokeSortBucket(int bucketId)
    {
        SortQueue->GetInvoker()->Invoke(BIND(&TSortingReader::DoSortBucket, MakeWeak(this), bucketId));
    }

    void InvokeMerge()
    {
        SortQueue->GetInvoker()->Invoke(BIND(&TSortingReader::DoMerge, MakeWeak(this)));
    }


    template <class TVector, class TItem>
    void SafePushBack(TVector& vector, TItem item)
    {
        if (vector.size() == vector.capacity()) {
            SortQueueBarrier();
            vector.reserve(static_cast<size_t>(vector.size() * ReallocationFactor));
        }
        vector.push_back(item);
    }

};

ISyncReaderPtr CreateSortingReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    IBlockCachePtr compressedBlockCache,
    IBlockCachePtr uncompressedBlockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TKeyColumns& keyColumns,
    TClosure onNetworkReleased,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunks,
    int estimatedRowCount,
    bool isApproximate)
{
    return New<TSortingReader>(
        config,
        masterChannel,
        compressedBlockCache,
        uncompressedBlockCache,
        nodeDirectory,
        keyColumns,
        onNetworkReleased,
        std::move(chunks),
        estimatedRowCount,
        isApproximate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

