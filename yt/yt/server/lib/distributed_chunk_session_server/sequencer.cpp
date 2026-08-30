#include "sequencer.h"

#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/options.h>

#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/hash.h>

#include <array>
#include <limits>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace NYT::NDistributedChunkSessionServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NJournalClient;
using namespace NLogging;

using NApi::NNative::IConnectionPtr;
using NApi::NNative::TClientOptions;

namespace {

////////////////////////////////////////////////////////////////////////////////

TError TryAddProgressCounter(
    i64* counter,
    i64 increment,
    std::string_view counterName) noexcept
{
    YT_VERIFY(*counter >= 0);
    YT_VERIFY(increment >= 0);

    if (increment > std::numeric_limits<i64>::max() - *counter) {
        return TError("Distributed chunk session progress counter overflow")
            .With("counter", std::string(counterName))
            .With("current_value", *counter)
            .With("increment", increment);
    }

    *counter += increment;
    return {};
}

TError TryAccumulateProgress(
    TDistributedChunkSessionProgress* progress,
    const TDistributedChunkSessionWriteStatistics& statistics,
    i64 compressedDataSize) noexcept
{
    struct TProgressCounterUpdate
    {
        i64* Counter = nullptr;
        i64 Increment = 0;
        std::string_view Name;
    };

    auto result = *progress;
    auto counterUpdates = std::to_array<TProgressCounterUpdate>({
        {&result.DataWeight, statistics.DataWeight, "data_weight"},
        {&result.CompressedDataSize, compressedDataSize, "compressed_data_size"},
        {&result.UncompressedDataSize, statistics.UncompressedDataSize, "uncompressed_data_size"},
        {&result.RowCount, statistics.RowCount, "row_count"},
        {&result.RecordCount, 1, "record_count"},
    });

    for (const auto& update : counterUpdates) {
        if (auto error = TryAddProgressCounter(
                update.Counter,
                update.Increment,
                update.Name);
            !error.IsOK())
        {
            return error;
        }
    }

    *progress = result;
    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionSequencer
    : public IDistributedChunkSessionSequencer
{
public:
    TDistributedChunkSessionSequencer(
        TSessionId sessionId,
        TChunkReplicaWithMediumList targets,
        TJournalChunkWriterOptionsPtr options,
        TJournalChunkWriterConfigPtr config,
        IConnectionPtr connection,
        IInvokerPtr invoker)
        : Writer_(CreateJournalChunkWriter(
            connection->CreateNativeClient(TClientOptions::Root()),
            sessionId,
            std::move(options),
            std::move(config),
            /*counters*/ {},
            std::move(invoker),
            std::move(targets),
            EChunkFormat::JournalDistributed,
            DistributedChunkSessionServiceLogger()))
        , Logger(DistributedChunkSessionServiceLogger().WithTag("SessionId", sessionId))
    { }

    TDistributedChunkSessionSequencer(
        TSessionId sessionId,
        IJournalChunkWriterPtr writer)
        : Writer_(std::move(writer))
        , Logger(DistributedChunkSessionServiceLogger().WithTag("SessionId", sessionId))
    { }

    TFuture<void> Open() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        {
            auto guard = Guard(StateLock_);
            YT_VERIFY(!std::exchange(OpenStarted_, true));
        }

        Writer_->SubscribeFailed(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSequencer::OnWriterFailed,
            MakeWeak(this)));

        auto result = Writer_->Open();
        result.Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSequencer::OnWriterOpened,
            MakeStrong(this)));
        return result.ToUncancelable();
    }

    TFuture<void> GetClosedFuture() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ClosedPromise_.ToFuture().AsVoid().ToUncancelable();
    }

    TFuture<void> WriteRecord(
        TSharedRef record,
        TDistributedChunkSessionWriteStatistics statistics) noexcept final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (statistics.DataWeight < 0 ||
            statistics.UncompressedDataSize < 0 ||
            statistics.RowCount < 0)
        {
            return MakeFuture<void>(TError("Distributed chunk session write statistics must be nonnegative")
                .With("data_weight", statistics.DataWeight)
                .With("uncompressed_data_size", statistics.UncompressedDataSize)
                .With("row_count", statistics.RowCount));
        }

        i64 compressedDataSize = std::ssize(record);

        {
            auto guard = Guard(StateLock_);
            if (!IsOpen_) {
                return MakeFuture<void>(TError("Journal writer is not open"));
            }
            if (Closing_) {
                return MakeFuture<void>(TError("Distributed chunk session is closing"));
            }
            if (PendingWriteCount_ == std::numeric_limits<int>::max()) {
                return MakeFuture<void>(TError("Too many pending distributed chunk session writes"));
            }
            // NB: Accounting is checked before the record is submitted, so the record that
            // would overflow the session counters is the one that gets refused.
            if (auto error = TryAccumulateProgress(
                    &AcceptedProgress_,
                    statistics,
                    compressedDataSize);
                !error.IsOK())
            {
                return MakeFuture<void>(error);
            }

            ++PendingSubmissionCount_;
            ++PendingWriteCount_;
        }

        YT_TLOG_DEBUG("Writing record")
            .With("DataWeight", statistics.DataWeight)
            .With("CompressedDataSize", compressedDataSize)
            .With("UncompressedDataSize", statistics.UncompressedDataSize)
            .With("RowCount", statistics.RowCount);

        // NB: A nonzero PendingSubmissionCount_ blocks TryClaimWriterClose, so
        // Writer_->Close() cannot be enqueued ahead of this submission and reject it;
        // WriteRecord is noexcept, so the count cannot leak.
        auto result = Writer_->WriteRecord(std::move(record))
            .Apply(BIND_NO_PROPAGATE(
                &TDistributedChunkSessionSequencer::OnWriteFinished,
                MakeStrong(this),
                statistics,
                compressedDataSize));

        auto [claimedWriterClose, finishedWrites] = [&] {
            auto guard = Guard(StateLock_);
            YT_VERIFY(PendingSubmissionCount_ > 0);
            --PendingSubmissionCount_;
            return std::pair(TryClaimWriterClose(), AreAllWritesFinished());
        }();

        if (finishedWrites) {
            AllWritesFinishedPromise_.TrySet();
        }
        if (claimedWriterClose) {
            CloseWriter();
        }

        return result.ToUncancelable();
    }

    TDistributedChunkSessionProgress GetProgress() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(StateLock_);
        return Progress_;
    }

    TFuture<TDistributedChunkSessionProgress> Close() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto [claimedWriterClose, finishedWrites] = [&] {
            auto guard = Guard(StateLock_);
            YT_VERIFY(OpenStarted_);
            Closing_ = true;
            return std::pair(TryClaimWriterClose(), AreAllWritesFinished());
        }();

        if (finishedWrites) {
            AllWritesFinishedPromise_.TrySet();
        }
        if (claimedWriterClose) {
            CloseWriter();
        }

        return ClosedPromise_.ToFuture().ToUncancelable();
    }

private:
    struct TConfirmedRecord
    {
        TDistributedChunkSessionWriteStatistics Statistics;
        i64 CompressedDataSize = 0;
    };

    const IJournalChunkWriterPtr Writer_;
    const TPromise<TDistributedChunkSessionProgress> ClosedPromise_ =
        NewPromise<TDistributedChunkSessionProgress>();
    const TPromise<void> AllWritesFinishedPromise_ = NewPromise<void>();
    const TLogger Logger;

    mutable YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StateLock_);
    TDistributedChunkSessionProgress Progress_;
    //! Progress every accepted record would add up to, checked before submission.
    TDistributedChunkSessionProgress AcceptedProgress_;
    THashMap<i64, TConfirmedRecord> ConfirmedRecords_;
    int PendingSubmissionCount_ = 0;
    int PendingWriteCount_ = 0;
    TError PendingWriteError_;
    bool OpenStarted_ = false;
    bool Closing_ = false;
    bool WriterCloseStarted_ = false;
    bool IsOpen_ = false;

    void OnWriteFinished(
        TDistributedChunkSessionWriteStatistics statistics,
        i64 compressedDataSize,
        const TErrorOr<i64>& recordIndexOrError)
    {
        auto [error, finishedWrites] = [&] {
            auto guard = Guard(StateLock_);

            TError error;
            if (recordIndexOrError.IsOK()) {
                AddConfirmedRecord(
                    recordIndexOrError.Value(),
                    TConfirmedRecord{
                        .Statistics = statistics,
                        .CompressedDataSize = compressedDataSize,
                    });
            } else {
                error = recordIndexOrError;
            }

            if (!error.IsOK() && PendingWriteError_.IsOK()) {
                PendingWriteError_ = error;
            }

            YT_VERIFY(PendingWriteCount_ > 0);
            --PendingWriteCount_;

            return std::pair(std::move(error), AreAllWritesFinished());
        }();

        if (finishedWrites) {
            AllWritesFinishedPromise_.TrySet();
        }

        if (error.IsOK()) {
            YT_TLOG_DEBUG("Record writing finished")
                .With("RecordIndex", recordIndexOrError.Value());
            return;
        }

        YT_TLOG_DEBUG("Record writing failed")
            .With(error);

        YT_UNUSED_FUTURE(Close());
        THROW_ERROR(error);
    }

    //! NB: Cannot overflow: every record was accounted for in AcceptedProgress_ before it
    //! was submitted, and the drained prefix is a subset of that.
    void AddConfirmedRecord(i64 recordIndex, TConfirmedRecord record)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(StateLock_);

        YT_VERIFY(recordIndex >= Progress_.RecordCount);
        YT_VERIFY(ConfirmedRecords_.emplace(recordIndex, std::move(record)).second);

        while (true) {
            auto recordIt = ConfirmedRecords_.find(Progress_.RecordCount);
            if (recordIt == ConfirmedRecords_.end()) {
                break;
            }

            const auto& confirmedRecord = recordIt->second;
            auto error = TryAccumulateProgress(
                &Progress_,
                confirmedRecord.Statistics,
                confirmedRecord.CompressedDataSize);
            YT_VERIFY(error.IsOK());

            ConfirmedRecords_.erase(recordIt);
        }
    }

    TDistributedChunkSessionProgress GetFinalProgress() const
    {
        auto [error, progress, acceptedWriteCount] = [&] {
            auto guard = Guard(StateLock_);
            return std::tuple(PendingWriteError_, Progress_, AcceptedProgress_.RecordCount);
        }();

        error.ThrowOnError();

        YT_TLOG_FATAL_IF(
            progress.RecordCount != acceptedWriteCount,
            "Successful journal record indexes do not form a contiguous prefix")
            .With("AcceptedWriteCount", acceptedWriteCount)
            .With("ConfirmedRecordCount", progress.RecordCount);

        return progress;
    }

    bool TryClaimWriterClose()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(StateLock_);

        if (!Closing_ || PendingSubmissionCount_ > 0 || WriterCloseStarted_) {
            return false;
        }

        WriterCloseStarted_ = true;
        return true;
    }

    bool AreAllWritesFinished() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(StateLock_);

        return Closing_ && PendingSubmissionCount_ == 0 && PendingWriteCount_ == 0;
    }

    void CloseWriter()
    {
        // NB: The writer close is issued right away so that the last partial batch is
        // flushed at once; the session ends only once every write is accounted for, even
        // when the close itself failed.
        ClosedPromise_.SetFrom(AllSet(std::vector{
            Writer_->Close(),
            AllWritesFinishedPromise_.ToFuture(),
        }).Apply(BIND_NO_PROPAGATE(
            [this, this_ = MakeStrong(this)] (const std::vector<TError>& errors) {
                errors[0].ThrowOnError();
                return GetFinalProgress();
            })));
    }

    //! A failed writer must not be used any more, so the session cannot stay open: without
    //! this the sequencer would keep answering pings until the next write or close.
    void OnWriterFailed(const TError& error)
    {
        YT_TLOG_DEBUG("Journal chunk writer failed, closing session")
            .With(error);

        YT_UNUSED_FUTURE(Close());
    }

    void OnWriterOpened(const TError& error)
    {
        if (error.IsOK()) {
            {
                auto guard = Guard(StateLock_);
                YT_VERIFY(!std::exchange(IsOpen_, true));
            }

            YT_TLOG_DEBUG("Journal chunk writer was opened");
            return;
        }

        YT_TLOG_DEBUG("Failed to open journal chunk writer")
            .With(error);

        YT_UNUSED_FUTURE(Close());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IDistributedChunkSessionSequencerPtr CreateDistributedChunkSessionSequencer(
    TSessionId sessionId,
    TChunkReplicaWithMediumList targets,
    TJournalChunkWriterOptionsPtr options,
    TJournalChunkWriterConfigPtr config,
    IConnectionPtr connection,
    IInvokerPtr invoker)
{
    return New<TDistributedChunkSessionSequencer>(
        sessionId,
        std::move(targets),
        std::move(options),
        std::move(config),
        std::move(connection),
        std::move(invoker));
}

IDistributedChunkSessionSequencerPtr CreateDistributedChunkSessionSequencerForTesting(
    TSessionId sessionId,
    IJournalChunkWriterPtr writer)
{
    return New<TDistributedChunkSessionSequencer>(
        sessionId,
        std::move(writer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
