#include <yt/yt/server/lib/distributed_chunk_session_server/sequencer.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/statistics.h>

#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <limits>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionSequencerPtr CreateDistributedChunkSessionSequencerForTesting(
    NChunkClient::TSessionId sessionId,
    NJournalClient::IJournalChunkWriterPtr writer);

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NJournalClient;

////////////////////////////////////////////////////////////////////////////////

class TControlledJournalChunkWriter
    : public IJournalChunkWriter
{
public:
    TFuture<void> Open() override
    {
        return BlockOpen_
            ? OpenPromise_.ToFuture()
            : OKFuture;
    }

    TFuture<void> Close() override
    {
        CloseCalledPromise_.TrySet();
        return ClosePromise_.ToFuture();
    }

    TFuture<i64> WriteRecord(TSharedRef record) noexcept override
    {
        Records_.push_back(std::move(record));
        auto promise = NewPromise<i64>();
        WritePromises_.push_back(promise);
        return promise.ToFuture();
    }

    TFuture<void> WriteEncodedRecordParts(std::vector<TSharedRef> /*recordParts*/) noexcept override
    {
        YT_ABORT();
    }

    bool IsCloseDemanded() const override
    {
        return false;
    }

    std::vector<NJournalClient::TChunkReplicaDescriptor> GetChunkReplicaDescriptors() const override
    {
        YT_ABORT();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Failed);

    void SetClosed()
    {
        ClosePromise_.Set();
    }

    void BlockOpen()
    {
        BlockOpen_ = true;
    }

    void SetOpened()
    {
        OpenPromise_.Set();
    }

    TFuture<void> GetCloseCalledFuture() const
    {
        return CloseCalledPromise_.ToFuture();
    }

    void SetCloseError(TError error)
    {
        ClosePromise_.Set(std::move(error));
    }

    void CompleteWrite(int writeIndex, i64 recordIndex)
    {
        WritePromises_[writeIndex].Set(recordIndex);
    }

    void Fail(TError error)
    {
        Failed_.Fire(std::move(error));
    }

    void FailWrite(int writeIndex, TError error)
    {
        WritePromises_[writeIndex].Set(std::move(error));
    }

private:
    const TPromise<void> OpenPromise_ = NewPromise<void>();
    const TPromise<void> ClosePromise_ = NewPromise<void>();
    const TPromise<void> CloseCalledPromise_ = NewPromise<void>();

    std::vector<TSharedRef> Records_;
    std::vector<TPromise<i64>> WritePromises_;
    bool BlockOpen_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributedChunkSessionSequencerTest, OpenIsUncancelable)
{
    auto writer = New<TControlledJournalChunkWriter>();
    writer->BlockOpen();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    auto openFuture = sequencer->Open();
    openFuture.Cancel(TError("Injected cancellation"));
    EXPECT_FALSE(openFuture.IsSet());

    writer->SetOpened();
    WaitFor(openFuture)
        .ThrowOnError();
}

//! The writer fires Failed on its own, for example when pinging a chunk replica fails,
//! and nothing else would notice until the next write or close.
TEST(TDistributedChunkSessionSequencerTest, WriterFailureClosesSession)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto closedFuture = sequencer->GetClosedFuture();
    EXPECT_FALSE(closedFuture.IsSet());

    writer->Fail(TError("Failed to ping journal chunk replica"));

    WaitFor(writer->GetCloseCalledFuture())
        .ThrowOnError();
    writer->SetCloseError(TError("Journal chunk writer failed"));

    EXPECT_FALSE(WaitFor(closedFuture).IsOK());
}

TEST(TDistributedChunkSessionSequencerTest, WriteRecordIsUncancelable)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto writeFuture = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("record")),
        TDistributedChunkSessionWriteStatistics{});
    writeFuture.Cancel(TError("Injected cancellation"));
    EXPECT_FALSE(writeFuture.IsSet());

    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);
    WaitFor(writeFuture)
        .ThrowOnError();
}

TEST(TDistributedChunkSessionSequencerTest, ProgressStopsBeforePendingRecord)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto firstWrite = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("aaa")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 10,
            .UncompressedDataSize = 30,
            .RowCount = 2,
        });
    auto secondWrite = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("bbbbb")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 20,
            .UncompressedDataSize = 50,
            .RowCount = 3,
        });

    writer->CompleteWrite(/*writeIndex*/ 1, /*recordIndex*/ 1);
    WaitFor(secondWrite)
        .ThrowOnError();
    EXPECT_EQ(sequencer->GetProgress(), TDistributedChunkSessionProgress{});

    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);
    WaitFor(firstWrite)
        .ThrowOnError();
    EXPECT_EQ(
        sequencer->GetProgress(),
        (TDistributedChunkSessionProgress{
            .DataWeight = 30,
            .CompressedDataSize = 8,
            .UncompressedDataSize = 80,
            .RecordCount = 2,
            .RowCount = 5,
        }));
}

TEST(TDistributedChunkSessionSequencerTest, CloseWaitsForProgressAccounting)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto firstWrite = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("first")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 11,
            .UncompressedDataSize = 31,
            .RowCount = 2,
        });
    auto secondWrite = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("second")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 13,
            .UncompressedDataSize = 37,
            .RowCount = 4,
        });

    auto closeFuture = sequencer->Close();
    writer->SetClosed();
    ASSERT_FALSE(closeFuture.IsSet());

    writer->CompleteWrite(/*writeIndex*/ 1, /*recordIndex*/ 1);
    ASSERT_FALSE(closeFuture.IsSet());
    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);

    auto progress = WaitFor(closeFuture)
        .ValueOrThrow();
    WaitFor(AllSucceeded(std::vector{firstWrite, secondWrite}))
        .ThrowOnError();
    EXPECT_EQ(
        progress,
        (TDistributedChunkSessionProgress{
            .DataWeight = 24,
            .CompressedDataSize = 11,
            .UncompressedDataSize = 68,
            .RecordCount = 2,
            .RowCount = 6,
        }));
}

//! The writer close is issued as soon as every record has been submitted, so the last
//! partial batch is flushed without waiting for the batch timer.
TEST(TDistributedChunkSessionSequencerTest, WriterIsClosedBeforePendingWritesFinish)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto writeFuture = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("record")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 7,
            .UncompressedDataSize = 12,
            .RowCount = 1,
        });

    auto closeFuture = sequencer->Close();

    WaitFor(writer->GetCloseCalledFuture())
        .ThrowOnError();
    EXPECT_FALSE(closeFuture.IsSet());

    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);
    WaitFor(writeFuture)
        .ThrowOnError();
    writer->SetClosed();

    auto progress = WaitFor(closeFuture)
        .ValueOrThrow();
    EXPECT_EQ(progress.RecordCount, 1);
}

TEST(TDistributedChunkSessionSequencerTest, CloseErrorWaitsForProgressAccounting)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto writeFuture = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("record")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 7,
            .UncompressedDataSize = 12,
            .RowCount = 1,
        });

    auto closeFuture = sequencer->Close();
    writer->SetCloseError(TError("Injected close failure"));
    ASSERT_FALSE(closeFuture.IsSet());

    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);
    WaitFor(writeFuture)
        .ThrowOnError();

    auto closeError = WaitFor(closeFuture);
    EXPECT_FALSE(closeError.IsOK());
    EXPECT_TRUE(closeError.GetMessage().contains("Injected close failure"));
}

TEST(TDistributedChunkSessionSequencerTest, PendingWriteErrorFailsSequencerClose)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto writeFuture = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("record")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 7,
            .UncompressedDataSize = 12,
            .RowCount = 1,
        });
    ASSERT_FALSE(writeFuture.IsSet());

    auto closeFuture = sequencer->Close();
    writer->SetClosed();
    ASSERT_FALSE(closeFuture.IsSet());

    auto weakSequencer = TWeakPtr(sequencer);
    sequencer.Reset();
    ASSERT_FALSE(weakSequencer.IsExpired());

    writer->FailWrite(/*writeIndex*/ 0, TError("Injected write failure"));

    auto writeError = WaitFor(writeFuture);
    EXPECT_FALSE(writeError.IsOK());
    EXPECT_TRUE(writeError.GetMessage().contains("Injected write failure"));

    auto closeError = WaitFor(closeFuture);
    EXPECT_FALSE(closeError.IsOK());
    EXPECT_TRUE(closeError.GetMessage().contains("Injected write failure"));

    writeFuture = {};
    closeFuture = {};
    EXPECT_TRUE(weakSequencer.IsExpired());
}

//! Accounting is checked before submission, so the record that would overflow the session
//! counters is refused outright and never reaches the journal.
TEST(TDistributedChunkSessionSequencerTest, RejectsProgressCounterOverflowWithoutPartialUpdate)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto firstWrite = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("a")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 10,
            .UncompressedDataSize = std::numeric_limits<i64>::max(),
            .RowCount = 2,
        });
    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);
    WaitFor(firstWrite)
        .ThrowOnError();

    auto progressBeforeOverflow = sequencer->GetProgress();

    auto writeError = WaitFor(sequencer->WriteRecord(
        TSharedRef::FromString(std::string("b")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 20,
            .UncompressedDataSize = 1,
            .RowCount = 3,
        }));
    EXPECT_FALSE(writeError.IsOK());
    EXPECT_TRUE(writeError.GetMessage().contains("overflow"));

    EXPECT_EQ(sequencer->GetProgress(), progressBeforeOverflow);

    auto closeFuture = sequencer->Close();
    writer->SetClosed();
    EXPECT_EQ(
        WaitFor(closeFuture).ValueOrThrow(),
        progressBeforeOverflow);
}

//! The refusal is decided against every accepted record, not just the drained prefix, so
//! it does not depend on the order in which earlier records are confirmed.
TEST(TDistributedChunkSessionSequencerTest, RejectsOverflowWhileEarlierRecordsArePending)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
        .ThrowOnError();

    auto firstWrite = sequencer->WriteRecord(
        TSharedRef::FromString(std::string("a")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 10,
            .UncompressedDataSize = std::numeric_limits<i64>::max() - 1,
            .RowCount = 1,
        });
    EXPECT_EQ(sequencer->GetProgress(), TDistributedChunkSessionProgress{});

    auto writeError = WaitFor(sequencer->WriteRecord(
        TSharedRef::FromString(std::string("b")),
        TDistributedChunkSessionWriteStatistics{
            .DataWeight = 20,
            .UncompressedDataSize = 2,
            .RowCount = 2,
        }));
    EXPECT_FALSE(writeError.IsOK());
    EXPECT_TRUE(writeError.GetMessage().contains("overflow"));

    writer->CompleteWrite(/*writeIndex*/ 0, /*recordIndex*/ 0);
    WaitFor(firstWrite)
        .ThrowOnError();

    auto closeFuture = sequencer->Close();
    writer->SetClosed();
    EXPECT_EQ(
        WaitFor(closeFuture).ValueOrThrow(),
        (TDistributedChunkSessionProgress{
            .DataWeight = 10,
            .CompressedDataSize = 1,
            .UncompressedDataSize = std::numeric_limits<i64>::max() - 1,
            .RecordCount = 1,
            .RowCount = 1,
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedChunkSessionServer
