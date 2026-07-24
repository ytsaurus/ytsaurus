#include "sort_reader.h"

#include "config.h"
#include "partition_reader.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/table_client/key.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/heap.h>

#include <library/cpp/yt/misc/compare.h>
#include <library/cpp/yt/misc/variant.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TInputRowsHolder
    : public TSharedRangeHolder
{
    std::vector<TShuffleReadBatchPtr> Batches;
};

////////////////////////////////////////////////////////////////////////////////

template <class TModePolicy>
class TSortReader
    : public ISortReader
{
public:
    TSortReader(
        TSortReaderConfigPtr config,
        IPushBasedPartitionReaderPtr underlyingReader,
        TComparator comparator,
        IInvokerPtr invoker,
        IInvokerPtr sortInvoker)
        : Config_(std::move(config))
        , UnderlyingReader_(std::move(underlyingReader))
        , Comparator_(std::move(comparator))
        , KeyColumnCount_(Comparator_.GetLength())
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , SortInvoker_(std::move(sortInvoker))
        , Logger(PushBasedShuffleLogger())
        , InputRowsHolder_(New<TInputRowsHolder>())
    {
        YT_VERIFY(Config_);
        YT_VERIFY(UnderlyingReader_);
        YT_VERIFY(SortInvoker_);
    }

    void Start()
    {
        SerializedInvoker_->Invoke(BIND_NO_PROPAGATE(&TSortReader::RequestNextBatch, MakeWeak(this)));
    }

    TFuture<TSharedRange<TUnversionedRow>> Read() override
    {
        auto promise = NewPromise<TSharedRange<TUnversionedRow>>();
        YT_VERIFY(!ReadInProgress_.exchange(true));
        auto future = promise.ToFuture();
        promise.OnCanceled(BIND_NO_PROPAGATE(
            &TSortReader::OnReadCanceled,
            MakeWeak(this))
            .Via(SerializedInvoker_));
        SerializedInvoker_->Invoke(
            BIND_NO_PROPAGATE(&TSortReader::DoRead, MakeWeak(this), Passed(std::move(promise))));
        return future;
    }

    void AddChunk(
        TChunkId chunkId,
        TChunkReplicaWithMediumList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex) override
    {
        UnderlyingReader_->AddChunk(
            chunkId,
            std::move(replicas),
            startRecordIndex,
            rangeEndRecordIndex);
    }

    void SetNoMoreChunks() override
    {
        UnderlyingReader_->SetNoMoreChunks();
    }

private:
    struct TBucket
    {
        std::vector<TUnversionedRow> Rows;
        // KeyColumnCount_ values per row.
        std::vector<TUnversionedValue> KeyValues;
        std::vector<i32> Order;
        int Position = 0;
    };

    const TSortReaderConfigPtr Config_;
    const IPushBasedPartitionReaderPtr UnderlyingReader_;
    const TComparator Comparator_;
    const int KeyColumnCount_;
    const IInvokerPtr SerializedInvoker_;
    const IInvokerPtr SortInvoker_;
    const TLogger Logger;

    std::atomic<bool> ReadInProgress_ = false;

    // All state below lives on SerializedInvoker_.

    std::unique_ptr<TBucket> CurrentBucket_;
    std::vector<std::unique_ptr<TBucket>> SealedBuckets_;
    TIntrusivePtr<TInputRowsHolder> InputRowsHolder_;

    TFuture<TShuffleReadBatchPtr> CurrentReadFuture_;
    int PendingSortCount_ = 0;
    bool IngestFinished_ = false;
    bool OutputStarted_ = false;

    std::vector<TBucket*> MergeHeap_;

    std::optional<std::vector<TUnversionedRow>> PreparedRows_;

    TPromise<TSharedRange<TUnversionedRow>> PendingReadPromise_;

    TError TerminalError_;

    void RequestNextBatch()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK()) {
            return;
        }
        CurrentReadFuture_ = UnderlyingReader_->Read();
        CurrentReadFuture_.Subscribe(
            BIND_NO_PROPAGATE(&TSortReader::OnBatchRead, MakeWeak(this))
                .Via(SerializedInvoker_));
    }

    void OnBatchRead(const TErrorOr<TShuffleReadBatchPtr>& batchOrError) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        CurrentReadFuture_ = {};
        if (!TerminalError_.IsOK()) {
            return;
        }
        if (!batchOrError.IsOK()) {
            Fail(TError("Error reading shuffle partition") << batchOrError);
            return;
        }
        const auto& batch = batchOrError.Value();

        bool anyRowIngested = false;
        for (const auto& record : batch->Records) {
            IngestRecord(record);
            anyRowIngested = anyRowIngested || !record.Rows.Empty();
        }
        if (anyRowIngested) {
            InputRowsHolder_->Batches.push_back(batch);
        }

        if (batch->Finished) {
            IngestFinished_ = true;
            SealCurrentBucket();
            MaybeStartOutput();
        } else {
            RequestNextBatch();
        }
    }

    void IngestRecord(const TShuffleReadRecord& record)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        for (i64 rowIndex = 0; rowIndex < std::ssize(record.Rows); ++rowIndex) {
            if (!CurrentBucket_) [[unlikely]] {
                CurrentBucket_ = std::make_unique<TBucket>();
                CurrentBucket_->Rows.reserve(Config_->BucketRowCount);
                CurrentBucket_->KeyValues.reserve(
                    static_cast<i64>(Config_->BucketRowCount) * KeyColumnCount_);
            }
            auto row = record.Rows[rowIndex];
            CurrentBucket_->Rows.push_back(row);
            ExtractKeyValues(row);
            if (std::ssize(CurrentBucket_->Rows) >= Config_->BucketRowCount) {
                SealCurrentBucket();
            }
        }
    }

    void ExtractKeyValues(TUnversionedRow row)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto& keyValues = CurrentBucket_->KeyValues;
        YT_VERIFY(KeyColumnCount_ <= static_cast<int>(row.GetCount()));
        keyValues.insert(keyValues.end(), row.Begin(), row.Begin() + KeyColumnCount_);
    }

    void SealCurrentBucket()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!CurrentBucket_) {
            return;
        }
        auto bucket = std::move(CurrentBucket_);
        bucket->Order.resize(bucket->Rows.size());
        std::iota(bucket->Order.begin(), bucket->Order.end(), 0);

        SealedBuckets_.push_back(std::move(bucket));
        auto* bucketPtr = SealedBuckets_.back().get();

        ++PendingSortCount_;
        BIND_NO_PROPAGATE(&TSortReader::SortBucket, MakeWeak(this), bucketPtr)
            .AsyncVia(SortInvoker_)
            .Run()
            .Subscribe(BIND_NO_PROPAGATE(&TSortReader::OnBucketSorted, MakeWeak(this))
                .Via(SerializedInvoker_));
    }

    void SortBucket(TBucket* bucket)
    {
        YT_ASSERT_INVOKER_AFFINITY(SortInvoker_);

        // Only Order is mutable after sealing.
        std::sort(
            bucket->Order.begin(),
            bucket->Order.end(),
            [&] (i32 lhs, i32 rhs) {
                return CompareEntries(*bucket, lhs, *bucket, rhs) < 0;
            });
    }

    void OnBucketSorted(const TError& error) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(PendingSortCount_ > 0);
        --PendingSortCount_;
        if (!error.IsOK()) {
            if (TerminalError_.IsOK()) {
                Fail(TError("Error sorting shuffle rows") << error);
            } else {
                MaybeReleaseFailedState();
            }
            return;
        }
        if (!TerminalError_.IsOK()) {
            MaybeReleaseFailedState();
            return;
        }
        MaybeStartOutput();
    }

    int CompareEntries(
        const TBucket& lhsBucket,
        i64 lhsIndex,
        const TBucket& rhsBucket,
        i64 rhsIndex) const
    {
        if (KeyColumnCount_ > 0) {
            TKey lhsKey(TRange(
                lhsBucket.KeyValues.data() + lhsIndex * KeyColumnCount_,
                static_cast<size_t>(KeyColumnCount_)));
            TKey rhsKey(TRange(
                rhsBucket.KeyValues.data() + rhsIndex * KeyColumnCount_,
                static_cast<size_t>(KeyColumnCount_)));
            int result = Comparator_.CompareKeys(lhsKey, rhsKey);
            if (result != 0) {
                return result;
            }
        }
        return TModePolicy::CompareTail(
            lhsBucket.Rows[lhsIndex],
            rhsBucket.Rows[rhsIndex]);
    }

    void MaybeStartOutput()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK()) {
            return;
        }
        if (!IngestFinished_ || PendingSortCount_ > 0 || OutputStarted_) {
            return;
        }
        OutputStarted_ = true;

        MergeHeap_.reserve(SealedBuckets_.size());
        for (const auto& bucket : SealedBuckets_) {
            YT_VERIFY(!bucket->Rows.empty());
            MergeHeap_.push_back(bucket.get());
        }
        try {
            MakeHeap(MergeHeap_.begin(), MergeHeap_.end(), MakeMergeComparator());
        } catch (const std::exception& ex) {
            Fail(TError("Error merging sorted shuffle rows") << TError(ex));
            return;
        }

        YT_LOG_DEBUG(
            "Sort reader entered output phase (BucketCount: %v)",
            std::ssize(SealedBuckets_));

        PrepareNextBatch();
    }

    void DoRead(const TPromise<TSharedRange<TUnversionedRow>>& promise)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK()) {
            SetReadPromise(promise, TerminalError_);
            return;
        }
        YT_VERIFY(!PendingReadPromise_);
        if (PreparedRows_) {
            ResolveRead(promise);
        } else {
            PendingReadPromise_ = promise;
        }
    }

    void OnReadCanceled(const TError& error) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        Fail(TError(NYT::EErrorCode::Canceled, "Sort reader canceled") << error);
    }

    void ResolveRead(const TPromise<TSharedRange<TUnversionedRow>>& promise)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);
        YT_VERIFY(PreparedRows_);

        auto preparedRows = std::exchange(PreparedRows_, {});
        try {
            auto rows = MakeSharedRange(
                std::move(*preparedRows),
                InputRowsHolder_);
            SetReadPromise(promise, std::move(rows));
            if (MergeHeap_.empty()) {
                ReleaseRetainedState();
            }
        } catch (const TErrorException& ex) {
            auto error = ex.Error();
            Fail(error);
            SetReadPromise(promise, error);
        } catch (const std::exception& ex) {
            auto error = TError("Error merging sorted shuffle rows") << TError(ex);
            Fail(error);
            SetReadPromise(promise, error);
        }
        PrepareNextBatch();
    }

    template <class TResult>
    void SetReadPromise(
        const TPromise<TSharedRange<TUnversionedRow>>& promise,
        TResult&& result)
    {
        YT_VERIFY(ReadInProgress_.exchange(false));
        promise.Set(std::forward<TResult>(result));
    }

    void PrepareNextBatch()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK() || PreparedRows_) {
            return;
        }

        try {
            auto preparedRows = ProduceBatch();
            if (!TerminalError_.IsOK()) {
                return;
            }
            PreparedRows_ = std::move(preparedRows);
        } catch (const TErrorException& ex) {
            Fail(ex.Error());
            return;
        } catch (const std::exception& ex) {
            Fail(TError("Error merging sorted shuffle rows") << TError(ex));
            return;
        }

        if (PendingReadPromise_) {
            auto promise = std::exchange(PendingReadPromise_, {});
            ResolveRead(promise);
        }
    }

    std::vector<TUnversionedRow> ProduceBatch()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TUnversionedRow> rows;
        i64 dataWeight = 0;
        auto yielder = CreatePeriodicYielder(Config_->MergeYieldPeriod);
        while (!MergeHeap_.empty() &&
            std::ssize(rows) < Config_->MaxRowsPerRead &&
            dataWeight < Config_->MaxDataWeightPerRead)
        {
            yielder.TryYield();
            if (!TerminalError_.IsOK()) {
                return {};
            }

            auto* bucket = MergeHeap_.front();
            i32 entryIndex = bucket->Order[bucket->Position];
            ++bucket->Position;
            if (bucket->Position == std::ssize(bucket->Order)) {
                ExtractHeap(MergeHeap_.begin(), MergeHeap_.end(), MakeMergeComparator());
                MergeHeap_.pop_back();
            } else {
                AdjustHeapFront(MergeHeap_.begin(), MergeHeap_.end(), MakeMergeComparator());
            }
            auto row = bucket->Rows[entryIndex];
            rows.push_back(row);
            dataWeight += GetDataWeight(row);
        }
        return rows;
    }

    auto MakeMergeComparator() const
    {
        return [this] (TBucket* lhs, TBucket* rhs) {
            return CompareEntries(
                *lhs,
                lhs->Order[lhs->Position],
                *rhs,
                rhs->Order[rhs->Position]) < 0;
        };
    }

    void Fail(TError error)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!TerminalError_.IsOK()) {
            return;
        }
        TerminalError_ = std::move(error);
        YT_LOG_DEBUG(TerminalError_, "Sort reader failed");
        if (CurrentReadFuture_) {
            auto future = std::exchange(CurrentReadFuture_, {});
            future.Cancel(TerminalError_);
        }
        if (PendingReadPromise_) {
            auto promise = std::exchange(PendingReadPromise_, {});
            SetReadPromise(promise, TerminalError_);
        }
        MaybeReleaseFailedState();
    }

    void MaybeReleaseFailedState()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (TerminalError_.IsOK() || PendingSortCount_ > 0) {
            return;
        }

        CurrentBucket_.reset();
        MergeHeap_.clear();
        PreparedRows_.reset();
        ReleaseRetainedState();
    }

    void ReleaseRetainedState()
    {
        SealedBuckets_.clear();
        InputRowsHolder_.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIdentityFreeModePolicy
{
public:
    static int CompareTail(TUnversionedRow /*lhs*/, TUnversionedRow /*rhs*/) noexcept
    {
        return 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIdentityPreservingModePolicy
{
public:
    static int CompareTail(TUnversionedRow lhs, TUnversionedRow rhs)
    {
        return TernaryCompare(GetRowIdentity(lhs), GetRowIdentity(rhs));
    }

private:
    static std::pair<i64, i64> GetRowIdentity(TUnversionedRow row)
    {
        YT_VERIFY(row.GetCount() >= IdentityColumnCount);
        auto identityIterator = row.End() - IdentityColumnCount;
        const auto& mapperId = *identityIterator++;
        const auto& rowId = *identityIterator;
        YT_ASSERT(mapperId.Type == EValueType::Int64);
        YT_ASSERT(rowId.Type == EValueType::Int64);
        return {mapperId.Data.Int64, rowId.Data.Int64};
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TModePolicy>
ISortReaderPtr CreateSortReaderWithPolicy(
    TSortReaderConfigPtr config,
    IPushBasedPartitionReaderPtr underlyingReader,
    TComparator comparator,
    TModePolicy /*modePolicy*/,
    IInvokerPtr invoker,
    IInvokerPtr sortInvoker)
{
    auto reader = New<TSortReader<TModePolicy>>(
        std::move(config),
        std::move(underlyingReader),
        std::move(comparator),
        std::move(invoker),
        std::move(sortInvoker));
    reader->Start();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISortReaderPtr CreateSortReaderForTesting(
    TSortReaderConfigPtr config,
    IPushBasedPartitionReaderPtr underlyingReader,
    TComparator comparator,
    TSortReaderMode mode,
    IInvokerPtr invoker,
    IInvokerPtr sortInvoker)
{
    auto createReader = [&] (auto modePolicy) {
        return CreateSortReaderWithPolicy(
            std::move(config),
            std::move(underlyingReader),
            std::move(comparator),
            std::move(modePolicy),
            std::move(invoker),
            std::move(sortInvoker));
    };

    return Visit(
        mode,
        [&] (const TValidMapperIds& /*validMapperIds*/) {
            return createReader(TIdentityFreeModePolicy());
        },
        [&] (const TIdentityColumnIds& /*identityColumnIds*/) {
            return createReader(TIdentityPreservingModePolicy());
        });
}

////////////////////////////////////////////////////////////////////////////////

ISortReaderPtr CreateSortReader(
    TSortReaderConfigPtr sortReaderConfig,
    TPartitionReaderConfigPtr partitionReaderConfig,
    NApi::NNative::IClientPtr client,
    TChunkReaderHostPtr chunkReaderHost,
    int readQuorum,
    TComparator comparator,
    TSortReaderMode mode,
    IInvokerPtr invoker,
    IInvokerPtr sortInvoker)
{
    auto createReader = [&] (
        auto modePolicy,
        TRecordHeaderFilter recordHeaderFilter,
        std::optional<TIdentityColumnIds> identityColumnIds)
    {
        auto partitionReader = CreatePushBasedPartitionReader(
            std::move(partitionReaderConfig),
            std::move(client),
            std::move(chunkReaderHost),
            readQuorum,
            invoker,
            std::move(recordHeaderFilter),
            std::move(identityColumnIds));
        return CreateSortReaderWithPolicy(
            std::move(sortReaderConfig),
            std::move(partitionReader),
            std::move(comparator),
            std::move(modePolicy),
            std::move(invoker),
            std::move(sortInvoker));
    };

    return Visit(
        std::move(mode),
        [&] (TValidMapperIds validMapperIds) {
            auto recordHeaderFilter = [
                validMapperIds = std::move(validMapperIds)] (const TRecordHeader& header)
            {
                return validMapperIds.contains(header.MapperId);
            };
            return createReader(
                TIdentityFreeModePolicy(),
                std::move(recordHeaderFilter),
                {});
        },
        [&] (TIdentityColumnIds identityColumnIds) {
            return createReader(
                TIdentityPreservingModePolicy(),
                {},
                std::move(identityColumnIds));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
