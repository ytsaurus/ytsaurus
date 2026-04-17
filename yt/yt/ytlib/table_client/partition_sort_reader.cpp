#include "partition_sort_reader.h"

#include "config.h"
#include "partition_chunk_reader.h"
#include "schemaless_block_reader.h"
#include "timing_reader.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/private.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>

#include <yt/yt/core/misc/heap.h>

#include <library/cpp/yt/memory/atomic.h>

#include <util/random/shuffle.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

using NRpc::IChannelPtr;
using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSortReaderTag
{ };

class TPartitionSortReader
    : public ISchemalessMultiChunkReader
    , public TTimingReaderBase
{
public:
    TPartitionSortReader(
        TMultiChunkReaderConfigPtr config,
        TChunkReaderHostPtr chunkReaderHost,
        TComparator comparator,
        TNameTablePtr nameTable,
        TClosure onNetworkReleased,
        TDataSourceDirectoryPtr dataSourceDirectory,
        std::vector<TDataSliceDescriptor> dataSliceDescriptors,
        i64 estimatedRowCount,
        bool approximate,
        const TPartitionTags& partitionTags,
        TClientChunkReadOptions chunkReadOptions,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
        : Comparator_(std::move(comparator))
        , OnNetworkReleased_(std::move(onNetworkReleased))
        , NameTable_(std::move(nameTable))
        , Approximate_(approximate)
        , EstimatedRowCount_(estimatedRowCount)
        , KeyBuffer_(this)
        , RowDescriptorBuffer_(this)
        , Buckets_(this)
        , BucketStartIndexes_(this)
        , SortComparator_(this)
        , MergeComparator_(this)
    {
        Shuffle(dataSliceDescriptors.begin(), dataSliceDescriptors.end());

        auto options = New<NTableClient::TTableReaderOptions>();
        options->KeepInMemory = true;

        UnderlyingReader_ = CreatePartitionMultiChunkReader(
            std::move(config),
            std::move(options),
            std::move(chunkReaderHost),
            std::move(dataSourceDirectory),
            std::move(dataSliceDescriptors),
            NameTable_,
            partitionTags,
            std::move(chunkReadOptions),
            std::move(multiReaderMemoryManager));

        SetReadyEvent(BIND(&TPartitionSortReader::DoOpen, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run());
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!IsReadyEventSetAndOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (!MergeError_.IsOK()) {
            SetReadyEvent(MakeFuture(MergeError_));
            return CreateEmptyUnversionedRowBatch();
        }

        if (ReadRowCount_ == TotalRowCount_) {
            WorkerQueue_->Shutdown();
            return nullptr;
        }

        while (CurrentSortedIndexesBatchPosition_ >= std::ssize(CurrentSortedIndexesBatch_)) {
            if (!NextSortedIndexesBatchFuture_) {
                NextSortedIndexesBatchFuture_ = SortedIndexesBatchQueue_.Dequeue();
            }
            if (!NextSortedIndexesBatchFuture_.IsSet()) {
                SetReadyEvent(NextSortedIndexesBatchFuture_.As<void>());
                return CreateEmptyUnversionedRowBatch();
            }
            auto batchOrError = std::exchange(NextSortedIndexesBatchFuture_, {})
                .AsUnique()
                .GetOrCrash();
            if (!batchOrError.IsOK()) {
                MergeError_ = TError(batchOrError);
                SetReadyEvent(MakeFuture(MergeError_));
                return CreateEmptyUnversionedRowBatch();
            }
            CurrentSortedIndexesBatch_ = std::move(batchOrError.Value());
            CurrentSortedIndexesBatchPosition_ = 0;
        }

        MemoryPool_.Clear();

        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        i64 dataWeight = 0;

        while (CurrentSortedIndexesBatchPosition_ < std::ssize(CurrentSortedIndexesBatch_) &&
            std::ssize(rows) < options.MaxRowsPerRead &&
            dataWeight < options.MaxDataWeightPerRead)
        {
            auto sortedIndex = CurrentSortedIndexesBatch_[CurrentSortedIndexesBatchPosition_++];
            const auto& rowDescriptor = RowDescriptorBuffer_[sortedIndex];
            YT_VERIFY(rowDescriptor.BlockReader->JumpToRowIndex(rowDescriptor.RowIndex));
            auto row = rowDescriptor.BlockReader->GetRow(&MemoryPool_, /*remapIds*/ true);
            rows.push_back(row);
            dataWeight += GetDataWeight(row);
        }

        YT_VERIFY(!rows.empty());

        SingleWriterFetchAdd(ReadRowCount_, std::ssize(rows));
        SingleWriterFetchAdd(ReadDataWeight_, dataWeight);

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_ABORT();
    }

    i64 GetSessionRowIndex() const override
    {
        return ReadRowCount_;
    }

    i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = UnderlyingReader_->GetDataStatistics();
        dataStatistics.set_row_count(ReadRowCount_);
        dataStatistics.set_data_weight(ReadDataWeight_);
        return dataStatistics;
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_ABORT();
    }

    void Interrupt() override
    {
        YT_ABORT();
    }

    void SkipCurrentReader() override
    {
        YT_ABORT();
    }

    i64 GetTableRowIndex() const override
    {
        // Not supported.
        return -1;
    }

private:
    class TComparatorBase
    {
    public:
        explicit TComparatorBase(TPartitionSortReader* reader)
            : Comparator_(reader->Comparator_)
            , KeyBuffer_(reader->KeyBuffer_)
        { }

    protected:
        const TComparator Comparator_;
        const std::vector<TUnversionedValue>& KeyBuffer_;

        //! Returns |True| iff row[lhs] < row[rhs].
        bool CompareRows(i64 lhs, i64 rhs) const
        {
            int keyColumnCount = Comparator_.GetLength();
            i64 lhsStartIndex = lhs * keyColumnCount;
            i64 rhsStartIndex = rhs * keyColumnCount;
            TKey lhsKey(TRange(KeyBuffer_.data() + lhsStartIndex, keyColumnCount));
            TKey rhsKey(TRange(KeyBuffer_.data() + rhsStartIndex, keyColumnCount));
            return Comparator_.CompareKeys(lhsKey, rhsKey) < 0;
        }
    };

    class TSortComparator
        : public TComparatorBase
    {
    public:
        explicit TSortComparator(TPartitionSortReader* reader)
            : TComparatorBase(reader)
        { }

        //! Returns |true| iff |row[lhs] < row[rhs]|.
        bool operator()(i64 lhs, i64 rhs) const
        {
            return CompareRows(lhs, rhs);
        }
    };

    class TMergeComparator
        : public TComparatorBase
    {
    public:
        explicit TMergeComparator(TPartitionSortReader* reader)
            : TComparatorBase(reader)
            , Buckets_(reader->Buckets_)
        { }

        //! Returns |true| iff |row[Buckets_[lhs]] < row[Buckets_[rhs]]|.
        bool operator()(int lhs, int rhs) const
        {
            return CompareRows(Buckets_[lhs], Buckets_[rhs]);
        }

    private:
        const std::vector<i32>& Buckets_;
    };

    template <class T>
    class TConcurrentVector
        : public std::vector<T>
    {
    public:
        explicit TConcurrentVector(TPartitionSortReader* reader)
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
        TPartitionSortReader* const UnderlyingReader_;

        static constexpr double ReallocationFactor = 1.1;

        void EnsureCapacity()
        {
            if (capacity() == size()) {
                UnderlyingReader_->SortBarrier();
                reserve(static_cast<size_t>(size() * ReallocationFactor));
            }
        }
    };

    const TComparator Comparator_;
    const TClosure OnNetworkReleased_;
    const TNameTablePtr NameTable_;

    const bool Approximate_;

    const i64 EstimatedRowCount_;
    int EstimatedBucketCount_ = -1;

    i64 TotalRowCount_ = 0;

    std::atomic<i64> ReadRowCount_ = 0;
    std::atomic<i64> ReadDataWeight_ = 0;

    // Sort
    TConcurrentVector<TUnversionedValue> KeyBuffer_;
    TConcurrentVector<TRowDescriptor> RowDescriptorBuffer_;
    TConcurrentVector<i32> Buckets_;
    TConcurrentVector<int> BucketStartIndexes_;

    // Merge
    std::vector<int> BucketHeap_;
    TNonblockingQueue<std::vector<i32>> SortedIndexesBatchQueue_;
    std::vector<i32> CurrentSortedIndexesBatch_;
    TFuture<std::vector<i32>> NextSortedIndexesBatchFuture_;
    int CurrentSortedIndexesBatchPosition_ = 0;
    TError MergeError_;

    TSortComparator SortComparator_;
    TMergeComparator MergeComparator_;

    TChunkedMemoryPool MemoryPool_{TPartitionSortReaderTag()};

    // TODO(babenko): consider using an externally-provided invoker.
    const TActionQueuePtr WorkerQueue_ = New<TActionQueue>("Worker");

    TPartitionMultiChunkReaderPtr UnderlyingReader_;

    // Sort error may occur due to CompositeValues in keys.
    std::vector<TFuture<void>> SortFutures_;

    static constexpr i32 BucketEndSentinel = -1;

    static constexpr int SortBucketSize = 10'000;
    static constexpr int MergeBatchSize = 10'000;

    void DoOpen()
    {
        InitInput();
        ReadInput();
        StartMerge();
    }

    void InitInput()
    {
        EstimatedBucketCount_ = (EstimatedRowCount_ + SortBucketSize - 1) / SortBucketSize;
        KeyBuffer_.reserve(EstimatedRowCount_ * Comparator_.GetLength());
        RowDescriptorBuffer_.reserve(EstimatedRowCount_);
        Buckets_.reserve(EstimatedRowCount_ + EstimatedBucketCount_);

        YT_LOG_INFO("Input size estimated (RowCount: %v, BucketCount: %v)",
            EstimatedRowCount_,
            EstimatedBucketCount_);
    }

    void ReadInput()
    {
        YT_LOG_INFO("Started reading input");

        bool isNetworkReleased = false;

        int bucketIndex = 0;
        int bucketSize = 0;
        int rowIndex = 0;

        auto flushBucket = [&] {
            Buckets_.push_back(BucketEndSentinel);
            BucketStartIndexes_.push_back(std::ssize(Buckets_));

            SortFutures_.push_back(InvokeSortBucket(bucketIndex));

            ++bucketIndex;
            bucketSize = 0;
        };

        BucketStartIndexes_.push_back(0);

        while (true) {
            i64 rowCount = 0;

            auto keyInserter = std::back_inserter(KeyBuffer_);
            auto rowDescriptorInserter = std::back_inserter(RowDescriptorBuffer_);

            auto result = UnderlyingReader_->Read(keyInserter, rowDescriptorInserter, &rowCount);
            if (!result) {
                break;
            }

            if (rowCount == 0) {
                WaitFor(UnderlyingReader_->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            // Push the row to the current bucket and flush the bucket if full.
            for (i64 i = 0; i < rowCount; ++i) {
                Buckets_.push_back(rowIndex++);
                ++bucketSize;
            }

            if (bucketSize >= SortBucketSize) {
                flushBucket();
            }

            if (!isNetworkReleased && UnderlyingReader_->IsFetchingCompleted()) {
                OnNetworkReleased_();
                isNetworkReleased =  true;
            }
        }

        if (bucketSize > 0) {
            flushBucket();
        }

        if (!isNetworkReleased) {
            YT_VERIFY(UnderlyingReader_->IsFetchingCompleted());
            OnNetworkReleased_();
        }

        TotalRowCount_ = rowIndex;
        int bucketCount = std::ssize(BucketStartIndexes_) - 1;

        if (!Approximate_) {
            YT_VERIFY(TotalRowCount_ <= EstimatedRowCount_);
            YT_VERIFY(bucketCount <= EstimatedBucketCount_);
        }

        YT_LOG_INFO("Finished reading input (RowCount: %v, BucketCount: %v)",
            TotalRowCount_,
            bucketCount);
    }

    void DoSortBucket(int bucketIndex)
    {
        YT_LOG_DEBUG("Started sorting bucket (Index: %v)", bucketIndex);

        int startIndex = BucketStartIndexes_[bucketIndex];
        int endIndex = BucketStartIndexes_[bucketIndex + 1] - 1;
        std::sort(Buckets_.begin() + startIndex, Buckets_.begin() + endIndex, SortComparator_);

        YT_LOG_DEBUG("Finished sorting bucket (Index: %v)", bucketIndex);
    }

    void StartMerge()
    {
        YT_LOG_INFO("Started waiting for sort");
        WaitFor(AllSucceeded(SortFutures_))
            .ThrowOnError();
        YT_LOG_INFO("Finished waiting for sort");

        InvokeMerge();
    }

    void DoMerge()
    {
        try {
            YT_LOG_INFO("Started merge");

            for (int index = 0; index < std::ssize(BucketStartIndexes_) - 1; ++index) {
                BucketHeap_.push_back(BucketStartIndexes_[index]);
            }
            MakeHeap(BucketHeap_.begin(), BucketHeap_.end(), MergeComparator_);

            std::vector<i32> sortedIndexesBatch;
            auto flushBatch = [&] {
                SortedIndexesBatchQueue_.Enqueue(std::exchange(sortedIndexesBatch, {}));
            };

            std::optional<i32> lastSortedIndex;

            while (!BucketHeap_.empty()) {
                int bucketIndex = BucketHeap_.front();
                auto sortedIndex = Buckets_[bucketIndex++];
                YT_ASSERT(!lastSortedIndex || !SortComparator_(sortedIndex, *lastSortedIndex));
                sortedIndexesBatch.push_back(sortedIndex);
                lastSortedIndex = sortedIndex;

                if (Buckets_[bucketIndex] == BucketEndSentinel) {
                    ExtractHeap(BucketHeap_.begin(), BucketHeap_.end(), MergeComparator_);
                    BucketHeap_.pop_back();
                } else {
                    BucketHeap_.front() = bucketIndex;
                    AdjustHeapFront(BucketHeap_.begin(), BucketHeap_.end(), MergeComparator_);
                }

                if (std::ssize(sortedIndexesBatch) >= MergeBatchSize) {
                    flushBatch();
                }
            }

            if (!sortedIndexesBatch.empty()) {
                flushBatch();
            }

            YT_LOG_INFO("Finished merge");
        } catch (const std::exception& ex) {
            SortedIndexesBatchQueue_.Enqueue(MakeFuture<std::vector<i32>>(ex));
        }
    }

    void SortBarrier()
    {
        BIND([] { }).AsyncVia(WorkerQueue_->GetInvoker())
            .Run()
            .BlockingGet();
    }

    TFuture<void> InvokeSortBucket(int bucketIndex)
    {
        return
            BIND(
                &TPartitionSortReader::DoSortBucket,
                MakeWeak(this),
                bucketIndex)
            .AsyncVia(WorkerQueue_->GetInvoker())
            .Run();
    }

    void InvokeMerge()
    {
        WorkerQueue_->GetInvoker()->Invoke(BIND(
            &TPartitionSortReader::DoMerge,
            MakeWeak(this)));
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreatePartitionSortReader(
    TMultiChunkReaderConfigPtr config,
    TChunkReaderHostPtr chunkReaderHost,
    TComparator comparator,
    TNameTablePtr nameTable,
    TClosure onNetworkReleased,
    TDataSourceDirectoryPtr dataSourceDirectory,
    std::vector<TDataSliceDescriptor> dataSliceDescriptors,
    i64 estimatedRowCount,
    bool approximate,
    const TPartitionTags& partitionTags,
    TClientChunkReadOptions chunkReadOptions,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    return New<TPartitionSortReader>(
        std::move(config),
        std::move(chunkReaderHost),
        std::move(comparator),
        std::move(nameTable),
        onNetworkReleased,
        std::move(dataSourceDirectory),
        std::move(dataSliceDescriptors),
        estimatedRowCount,
        approximate,
        partitionTags,
        std::move(chunkReadOptions),
        std::move(multiReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
