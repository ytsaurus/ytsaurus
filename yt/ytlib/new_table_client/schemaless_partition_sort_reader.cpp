#include "stdafx.h"

#include "schemaless_partition_sort_reader.h"

#include "config.h"
#include "partition_chunk_reader.h"
#include "private.h"
#include "schemaless_block_reader.h"

#include <core/profiling/profiler.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/scheduler.h>

#include <core/misc/heap.h>
#include <core/misc/varint.h>

#include <util/system/yield.h>


namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

using NRpc::IChannelPtr;
using NNodeTrackerClient::TNodeDirectoryPtr;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TableClientLogger;
static auto& Profiler = TableReaderProfiler;

static const int SortBucketSize = 10000;
static const int SpinsBetweenYield = 1000;
static const int RowsBetweenAtomicUpdate = 10000;
static const i32 BucketEndSentinel = -1;
static const double ReallocationFactor = 1.1;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessPartitionSortReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessPartitionSortReader(
        TMultiChunkReaderConfigPtr config,
        IChannelPtr masterChannel,
        IBlockCachePtr compressedBlockCache,
        IBlockCachePtr uncompressedBlockCache,
        TNodeDirectoryPtr nodeDirectory,
        const TKeyColumns& keyColumns,
        TNameTablePtr nameTable,
        TClosure onNetworkReleased,
        std::vector<TChunkSpec> chunks,
        int estimatedRowCount,
        bool isApproximate)
        : KeyColumns_(keyColumns)
        , KeyColumnCount_(static_cast<int>(KeyColumns_.size()))
        , OnNetworkReleased_(onNetworkReleased)
        , IsApproximate_(isApproximate)
        , EstimatedRowCount_(estimatedRowCount)
        , TotalRowCount_(0)
        , ReadRowCount_(0)
        , KeyBuffer_(this)
        , RowPtrBuffer_(this)
        , Buckets_(this)
        , BucketStart_(this)
        , SortComparer_(this)
        , MergeComparer_(this)
    {
        YCHECK(EstimatedRowCount_ <= std::numeric_limits<i32>::max());

        srand(time(nullptr));
        std::random_shuffle(chunks.begin(), chunks.end());

        auto options = New<TMultiChunkReaderOptions>();
        options->KeepInMemory = true;

        UnderlyingReader_ = New<TPartitionMultiChunkReader>(
            config,
            options,
            masterChannel,
            compressedBlockCache,
            uncompressedBlockCache,
            nodeDirectory,
            std::move(chunks),
            nameTable,
            KeyColumns_);
    }

    virtual TAsyncError Open() override
    {
        SortQueue_ = New<TActionQueue>("Sort");

        try {
            InitInput();
            ReadInput();
            StartMerge();
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }

        return MakeFuture(TError());
    }

    virtual bool Read(std::vector<TUnversionedRow> *rows) override
    {
        MemoryPool_.Clear();
        rows->clear();

        if (ReadRowCount_ == TotalRowCount_) {
            SortQueue_->Shutdown();
            return false;
        }

        i64 sortedRowCount = SortedRowCount_;
        for (int spinCounter = 1; ; ++spinCounter) {
            if (sortedRowCount > ReadRowCount_) {
                break;
            }
            if (spinCounter % SpinsBetweenYield == 0) {
                ThreadYield();
            } else {
                SpinLockPause();
            }

            sortedRowCount = AtomicGet(SortedRowCount_);
        }

        while (ReadRowCount_ < sortedRowCount) {
            auto sortedIndex = SortedIndexes_[ReadRowCount_];
            const char* rowPtr = RowPtrBuffer_[sortedIndex];
            rows->push_back(THorizontalSchemalessBlockReader::GetRow(rowPtr, &MemoryPool_));

            ++ReadRowCount_;
        }

        YCHECK(!rows->empty());
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        YUNREACHABLE();
    }

    virtual int GetTableIndex() const override
    {
        return 0;
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return ReadRowCount_;
    }

    virtual i64 GetSessionRowCount() const override
    {
        return TotalRowCount_;
    }

    virtual TNameTablePtr GetNameTable() const override
    {
        return UnderlyingReader_->GetNameTable();
    }

    virtual bool IsFetchingCompleted() const override
    {
        YCHECK(UnderlyingReader_);
        return UnderlyingReader_->IsFetchingCompleted();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        YCHECK(UnderlyingReader_);
        auto dataStatistics = UnderlyingReader_->GetDataStatistics();
        dataStatistics.set_row_count(ReadRowCount_);
        return dataStatistics;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YCHECK(UnderlyingReader_);
        return UnderlyingReader_->GetFailedChunkIds();
    }

private:
    class TComparerBase
    {
    public:
        explicit TComparerBase(TSchemalessPartitionSortReader* reader)
            : KeyColumnCount_(reader->KeyColumnCount_)
            , KeyBuffer_(reader->KeyBuffer_)
        { }

    protected:
        int KeyColumnCount_;
        std::vector<TUnversionedValue>& KeyBuffer_;

        bool CompareRows(i32 lhs, i32 rhs) const
        {
            int lhsStartIndex = lhs * KeyColumnCount_;
            int lhsEndIndex   = lhsStartIndex + KeyColumnCount_;
            int rhsStartIndex = rhs * KeyColumnCount_;
            for (int lhsIndex = lhsStartIndex, rhsIndex = rhsStartIndex;
                lhsIndex < lhsEndIndex;
                ++lhsIndex, ++rhsIndex)
            {
                auto res = CompareRowValues(KeyBuffer_[lhsIndex], KeyBuffer_[rhsIndex]);
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
        explicit TSortComparer(TSchemalessPartitionSortReader* reader)
            : TComparerBase(reader)
        { }

        // Returns True iff row[lhs] < row[rhs]
        bool operator () (i32 lhs, i32 rhs) const
        {
            return CompareRows(lhs, rhs);
        }
    };

    class TMergeComparer
        : public TComparerBase
    {
    public:
        explicit TMergeComparer(TSchemalessPartitionSortReader* reader)
            : TComparerBase(reader)
            , Buckets_(reader->Buckets_)
        { }

        // Returns True iff row[Buckets[lhs]] < row[Buckets[rhs]]
        bool operator () (int lhs, int rhs) const
        {
            return CompareRows(Buckets_[lhs], Buckets_[rhs]);
        }

    private:
        std::vector<i32>& Buckets_;

    };

    template <class T>
    class TSafeVector
        : public std::vector<T>
    {
    public:
        explicit TSafeVector(TSchemalessPartitionSortReader* reader)
            : UnderlyingReader_(reader)
        { }

        void push_back(const T& value)
        {
            EnsureCapacity();
            std::vector<T>::push_back(value);
        }

        void push_back(T&& value)
        {
            EnsureCapacity();
            std::vector<T>::push_back(std::move(value));
        }

        using std::vector<T>::capacity;
        using std::vector<T>::reserve;
        using std::vector<T>::size;

    private:
        TSchemalessPartitionSortReader* UnderlyingReader_;

        void EnsureCapacity()
        {
            if (capacity() == size()) {
                UnderlyingReader_->SortQueueBarrier();
                reserve(static_cast<size_t>(size() * ReallocationFactor));
            }
        }

    };


    TKeyColumns KeyColumns_;
    int KeyColumnCount_;
    TClosure OnNetworkReleased_;

    TPartitionMultiChunkReaderPtr UnderlyingReader_;
    TActionQueuePtr SortQueue_;

    TChunkedMemoryPool MemoryPool_;

    bool IsApproximate_;

    int EstimatedRowCount_;
    int EstimatedBucketCount_;

    int TotalRowCount_;
    TAtomic SortedRowCount_;
    int ReadRowCount_;

    TSafeVector<TUnversionedValue> KeyBuffer_;
    TSafeVector<const char*> RowPtrBuffer_;
    TSafeVector<i32> Buckets_;
    TSafeVector<int> BucketStart_;

    std::vector<int> BucketHeap_;
    std::vector<i32> SortedIndexes_;

    TSortComparer SortComparer_;
    TMergeComparer MergeComparer_;


    void InitInput()
    {
        LOG_INFO("Initializing input");
        PROFILE_TIMING ("/reduce/init_time") {
            auto error = WaitFor(UnderlyingReader_->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to open partition reader");

            EstimatedBucketCount_ = (EstimatedRowCount_ + SortBucketSize - 1) / SortBucketSize;
            LOG_INFO("Input size estimated (RowCount: %d, BucketCount: %d)",
                EstimatedRowCount_,
                EstimatedBucketCount_);

            KeyBuffer_.reserve(EstimatedRowCount_ * KeyColumnCount_);
            RowPtrBuffer_.reserve(EstimatedRowCount_);
            Buckets_.reserve(EstimatedRowCount_ + EstimatedBucketCount_);
        }
    }

    void ReadInput()
    {
        LOG_INFO("Started reading input");
        PROFILE_TIMING ("/reduce/read_time" ) {
            bool isNetworkReleased = false;

            int bucketId = 0;
            int bucketSize = 0;
            int rowIndex = 0;

            auto flushBucket = [&] () {
                Buckets_.push_back(BucketEndSentinel);
                BucketStart_.push_back(Buckets_.size());

                InvokeSortBucket(bucketId);
                ++bucketId;
                bucketSize = 0;
            };

            BucketStart_.push_back(0);

            while (true) {
                i64 rowCount = 0;
                auto keyInserter = std::back_inserter(KeyBuffer_);
                auto rowPtrInserter = std::back_inserter(RowPtrBuffer_);

                auto result = UnderlyingReader_->Read(keyInserter, rowPtrInserter, &rowCount);
                if (!result)
                    break;

                if (!rowCount) {
                    auto error = WaitFor(UnderlyingReader_->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                    continue;
                }

                // Push the row to the current bucket and flush the bucket if full.
                for (i64 i = 0; i < rowCount; ++i) {
                    Buckets_.push_back(rowIndex);
                    ++rowIndex;
                    ++bucketSize;
                }

                if (bucketSize >= SortBucketSize) {
                    flushBucket();
                }

                if (!isNetworkReleased && UnderlyingReader_->IsFetchingCompleted()) {
                    OnNetworkReleased_.Run();
                    isNetworkReleased =  true;
                }
            }

            if (bucketSize > 0) {
                flushBucket();
            }

            YCHECK(isNetworkReleased);

            TotalRowCount_ = rowIndex;
            int bucketCount = static_cast<int>(BucketStart_.size()) - 1;

            if (!IsApproximate_) {
                YCHECK(TotalRowCount_ <= EstimatedRowCount_);
                YCHECK(bucketCount <= EstimatedBucketCount_);
            }

            LOG_INFO("Finished reading input (RowCount: %d, BucketCount: %d)",
                TotalRowCount_,
                bucketCount);
        }
    }

    void DoSortBucket(int bucketId)
    {
        LOG_DEBUG("Started sorting bucket %d", bucketId);

        int startIndex = BucketStart_[bucketId];
        int endIndex = BucketStart_[bucketId + 1] - 1;
        std::sort(Buckets_.begin() + startIndex, Buckets_.begin() + endIndex, SortComparer_);

        LOG_DEBUG("Finished sorting bucket %d", bucketId);
    }

    void StartMerge()
    {
        LOG_INFO("Waiting for sort thread");
        PROFILE_TIMING ("/reduce/sort_wait_time") {
            SortQueueBarrier();
        }
        LOG_INFO("Sort thread is idle");

        SortedIndexes_.reserve(TotalRowCount_);

        for (int index = 0; index < static_cast<int>(BucketStart_.size()) - 1; ++index) {
            BucketHeap_.push_back(BucketStart_[index]);
        }

        MakeHeap(BucketHeap_.begin(), BucketHeap_.end(), MergeComparer_);

        AtomicSet(SortedRowCount_, 0);
        ReadRowCount_ = 0;

        InvokeMerge();
    }

    void DoMerge()
    {
        LOG_INFO("Started merge");
        PROFILE_TIMING ("/reduce/merge_time") {
            int sortedRowCount = 0;
            while (!BucketHeap_.empty()) {
                int bucketIndex = BucketHeap_.front();
                if (SortedIndexes_.size() > 0) {
                    YASSERT(!SortComparer_(Buckets_[bucketIndex], SortedIndexes_.back()));
                }
                SortedIndexes_.push_back(Buckets_[bucketIndex]);
                ++bucketIndex;
                if (Buckets_[bucketIndex] == BucketEndSentinel) {
                    ExtractHeap(BucketHeap_.begin(), BucketHeap_.end(), MergeComparer_);
                    BucketHeap_.pop_back();
                } else {
                    BucketHeap_.front() = bucketIndex;
                    AdjustHeapFront(BucketHeap_.begin(), BucketHeap_.end(), MergeComparer_);
                }

                ++sortedRowCount;
                if (sortedRowCount % RowsBetweenAtomicUpdate == 0) {
                    AtomicSet(SortedRowCount_, sortedRowCount);
                }
            }

            YCHECK(sortedRowCount == TotalRowCount_);
            AtomicSet(SortedRowCount_, sortedRowCount);
        }
        LOG_INFO("Finished merge");
    }

    void SortQueueBarrier()
    {
        BIND([] () { }).AsyncVia(SortQueue_->GetInvoker()).Run().Get();
    }

    void InvokeSortBucket(int bucketId)
    {
        SortQueue_->GetInvoker()->Invoke(BIND(
            &TSchemalessPartitionSortReader::DoSortBucket,
            MakeWeak(this),
            bucketId));
    }

    void InvokeMerge()
    {
        SortQueue_->GetInvoker()->Invoke(BIND(
            &TSchemalessPartitionSortReader::DoMerge,
            MakeWeak(this)));
    }

};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessPartitionSortReader(
    TMultiChunkReaderConfigPtr config,
    IChannelPtr masterChannel,
    IBlockCachePtr compressedBlockCache,
    IBlockCachePtr uncompressedBlockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TKeyColumns& keyColumns,
    TNameTablePtr nameTable,
    TClosure onNetworkReleased,
    const std::vector<TChunkSpec>& chunks,
    i64 estimatedRowCount,
    bool isApproximate)
{
    return New<TSchemalessPartitionSortReader>(
        config,
        masterChannel,
        compressedBlockCache,
        uncompressedBlockCache,
        nodeDirectory,
        keyColumns,
        nameTable,
        onNetworkReleased,
        chunks,
        estimatedRowCount,
        isApproximate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

