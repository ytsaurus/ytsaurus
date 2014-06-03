#include "stdafx.h"

#include "schemaless_sorting_reader.h"

#include "partition_chunk_reader.h"
#include "private.h"

#include <core/profiling/profiler.h>

#include <core/concurrency/scheduler.h>

/*
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
*/

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

/*
using NVersionedTableClient::TKey;
using NVersionedTableClient::EValueType;
using NVersionedTableClient::MakeUnversionedSentinelValue;
*/

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TableReaderLogger;
static auto& Profiler = TableReaderProfiler;

static const int SortBucketSize = 10000;
static const int SpinsBetweenYield = 1000;
static const int RowsBetweenAtomicUpdate = 10000;
static const i32 BucketEndSentinel = -1;
static const double ReallocationFactor = 1.1;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSortingReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessSortingReader(
        TMultiChunkReaderConfigPtr config,
        IChannelPtr masterChannel,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const TKeyColumns& keyColumns,
        TNameTablePtr nameTable,
        TClosure onNetworkReleased,
        std::vector<TChunkSpec>&& chunks,
        int estimatedRowCount,
        bool isApproximate)
        : KeyColumns_(keyColumns)
        , KeyColumnCount_(static_cast<int>(KeyColumns_.size()))
        , OnNetworkReleased_(onNetworkReleased)
        , IsApproximate_(isApproximate)
        , EstimatedRowCount_(estimatedRowCount)
        , TotalRowCount_(0)
        , ReadRowCount_(0)
        , CurrentKey_(TKey::Allocate(&KeyMemoryPool, KeyColumnCount_))
        , SortComparer_(this)
        , MergeComparer_(this)
    {
        YCHECK(EstimatedRowCount_ <= std::numeric_limits<i32>::max());

        srand(time(nullptr));
        std::random_shuffle(chunks.begin(), chunks.end());

        auto options = New<TMultiChunkReaderOptions>();
        options->KeepInMemory = true;
        Reader_ = CreatePartitionParallelMultiChunkReader(
            config,
            options,
            masterChannel,
            blockCache,
            nodeDirectory,
            chunks,
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
        YUNIMPLEMENTED();
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return Reader_->GetReadyEvent();
    }


/*    virtual const TRow* GetRow() override
    {
        DoNextRow();
        return IsValid_ ? &CurrentRow : nullptr;
    }

    virtual const TKey& GetKey() const override
    {
        return CurrentKey_;
    }

    virtual i64 GetSessionRowCount() const override
    {
        return TotalRowCount_;
    }

    virtual i64 GetTableRowIndex() const override
    {
        YUNREACHABLE();
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return ReadRowCount_;
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
*/

private:
    class TComparerBase
    {
    public:
        explicit TComparerBase(TSortingReader* reader)
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
        explicit TSortComparer(TSortingReader* reader)
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
        explicit TMergeComparer(TSortingReader* reader)
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
        explicit TSafeVector(TSchemalessSortingReader* reader)
            : Reader_(reader)
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

    private:
        TSchemalessSortingReader* Reader_;

        void EnsureCapacity()
        {
            if (capacity() == size()) {
                Reader_->SortQueueBarrier();
                reserve(static_cast<size_t>(size() * ReallocationFactor));
            }
        }

    };


    TKeyColumns KeyColumns_;
    int KeyColumnCount_;
    TClosure OnNetworkReleased_;

    IPartitionMultiChunkReaderPtr Reader_;
    TActionQueuePtr SortQueue_;

    bool IsApproximate_;

    int EstimatedRowCount_;
    int EstimatedBucketCount_;

    int TotalRowCount_;
    TAtomic SortedRowCount_;
    int ReadRowCount_;

    std::vector<TUnversionedValue> KeyBuffer_;
    std::vector<const char*> RowPtrBuffer_;
    std::vector<i32> Buckets_;
    std::vector<i32> SortedIndexes_;
    std::vector<int> BucketStart_;
    std::vector<int> BucketHeap_;

    TSortComparer SortComparer_;
    TMergeComparer MergeComparer_;


    void InitInput()
    {
        LOG_INFO("Initializing input");
        PROFILE_TIMING ("/reduce/init_time") {
            auto error = WaitFor(Reader_->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to open partition reader");

            EstimatedBucketCount_ = (EstimatedRowCount_ + SortBucketSize - 1) / SortBucketSize;
            LOG_INFO("Input size estimated (RowCount: %d, BucketCount: %d)",
                EstimatedRowCount_,
                EstimatedBucketCount_);

            KeyBuffer_.reserve(EstimatedRowCount_ * KeyColumnCount_);
            RowPtrBuffer_.reserve(EstimatedRowCount_);
            Buckets_.reserve(EstimatedRowCount_ + EstimatedBucketCount_);
            SortedIndexes_.reserve(EstimatedRowCount_);
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
                SafePushBack(Buckets_, BucketEndSentinel);
                SafePushBack(BucketStart_, Buckets_.size());
                InvokeSortBucket(bucketId);
                ++bucketId;
                bucketSize = 0;
            };

            SafePushBack(BucketStart_, 0);

            while (true) {
                i64 rowCount = 0;
                auto keyInserter = std::back_inserter(KeyBuffer_);
                auto rowPtrInserter = std::back_inserter(RowPtrBuffer_);

                i64 maxRowCount = RowPtrBuffer_.size() - rowIndex - 1;
                auto result = Reader_->Read(maxRowCount, keyInserter, rowPtrInserter, &rowCount);
                if (!result)
                    break;

                if (!rowCount) {
                    auto error = WaitFor(Reader_->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                    continue;
                }

                // Push the row to the current bucket and flush the bucket if full.
                for (i64 i = 0; i < rowCount; ++i) {
                    SafePushBack(Buckets_, rowIndex);
                    ++rowIndex;
                    ++bucketSize;
                }

                if (bucketSize >= SortBucketSize) {
                    flushBucket();
                }

                if (!isNetworkReleased && Reader->GetIsFetchingComplete()) {
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

    void DoNextRow()
    {
        // SortQueue_->Shutdown();

        if (ReadRowCount_ == TotalRowCount_) {
            SetInvalid();
            return;
        }

        int currentIndex = ReadRowCount_;
        for (int spinCounter = 1; ; ++spinCounter) {
            auto sortedRowCount = spinCounter == 1 ? SortedRowCount_ : AtomicGet(SortedRowCount_);
            if (sortedRowCount > currentIndex) {
                break;
            }
            if (spinCounter % SpinsBetweenYield == 0) {
                ThreadYield();
            } else {
                SpinLockPause();
            }
        }

        auto sortedIndex = SortedIndexes_[currentIndex];

        // Prepare key.
        ClearKey();
        for (int index = 0; index < KeyColumnCount_; ++index) {
            const auto& keyPart = KeyBuffer_[sortedIndex * KeyColumnCount_ + index];
            CurrentKey_[index] = MakeKeyPart(keyPart);
        }

        // Prepare row.
        CurrentRow.clear();
        RowInput.Reset(RowPtrBuffer_[sortedIndex], std::numeric_limits<size_t>::max());
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

        ++ReadRowCount_;
    }

    void SortQueueBarrier()
    {
        BIND([] () { }).AsyncVia(SortQueue_->GetInvoker()).Run().Get();
    }

    void InvokeSortBucket(int bucketId)
    {
        SortQueue_->GetInvoker()->Invoke(BIND(
            &TSchemalessSortingReader::DoSortBucket,
            MakeWeak(this),
            bucketId));
    }

    void InvokeMerge()
    {
        SortQueue_->GetInvoker()->Invoke(BIND(
            &TSchemalessSortingReader::DoMerge,
            MakeWeak(this)));
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
    NChunkClient::IBlockCachePtr blockCache,
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
        blockCache,
        nodeDirectory,
        keyColumns,
        onNetworkReleased,
        std::move(chunks),
        estimatedRowCount,
        isApproximate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

