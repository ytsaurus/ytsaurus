#include <yt/yt/server/lib/distributed_chunk_session_server/distributed_chunk_session_sequencer.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionSequencerPtr CreateDistributedChunkSessionSequencerForTesting(
    NChunkClient::TSessionId sessionId,
    NJournalClient::IJournalChunkWriterPtr writer);

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NJournalClient;

////////////////////////////////////////////////////////////////////////////////

class TControlledJournalChunkWriter
    : public IJournalChunkWriter
{
public:
    TFuture<void> Open() override
    {
        return OpenPromise_.ToFuture();
    }

    TFuture<void> Close() override
    {
        return ClosePromise_.ToFuture();
    }

    TFuture<i64> WriteRecord(TSharedRef record) override
    {
        Records_.push_back(std::move(record));
        return WritePromise_.ToFuture();
    }

    TFuture<void> WriteEncodedRecordParts(std::vector<TSharedRef> /*recordParts*/) override
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

    void SetOpened()
    {
        OpenPromise_.Set();
    }

    void SetWriteSucceeded()
    {
        WritePromise_.Set(0);
    }

    void SetWriteError(TError error)
    {
        WritePromise_.Set(std::move(error));
    }

private:
    const TPromise<void> OpenPromise_ = NewPromise<void>();
    const TPromise<void> ClosePromise_ = NewPromise<void>();
    const TPromise<i64> WritePromise_ = NewPromise<i64>();

    std::vector<TSharedRef> Records_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributedChunkSessionSequencerTest, OpenIsUncancelable)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    auto openFuture = sequencer->Open();
    openFuture.Cancel(TError("Injected cancellation"));
    EXPECT_FALSE(openFuture.IsSet());

    writer->SetOpened();
    WaitFor(openFuture)
        .ThrowOnError();
}

TEST(TDistributedChunkSessionSequencerTest, WriteRecordIsUncancelable)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    auto openFuture = sequencer->Open();
    writer->SetOpened();
    WaitFor(openFuture)
        .ThrowOnError();

    auto writeFuture = sequencer->WriteRecord(TSharedRef::FromString(std::string("record")));
    writeFuture.Cancel(TError("Injected cancellation"));
    EXPECT_FALSE(writeFuture.IsSet());

    writer->SetWriteSucceeded();
    WaitFor(writeFuture)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributedChunkSessionSequencerTest, PendingWriteErrorIsPropagatedAfterSequencerClose)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    auto openFuture = sequencer->Open();
    writer->SetOpened();
    WaitFor(openFuture)
        .ThrowOnError();

    auto writeFuture = sequencer->WriteRecord(TSharedRef::FromString(std::string("record")));
    ASSERT_FALSE(writeFuture.IsSet());

    auto closeFuture = sequencer->Close();
    writer->SetClosed();
    WaitFor(closeFuture)
        .ThrowOnError();

    auto weakSequencer = TWeakPtr(sequencer);
    sequencer.Reset();
    ASSERT_TRUE(weakSequencer.IsExpired());

    writer->SetWriteError(TError("Injected write failure"));

    auto writeError = WaitFor(writeFuture);
    EXPECT_FALSE(writeError.IsOK());
    EXPECT_TRUE(writeError.GetMessage().contains("Injected write failure"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedChunkSessionServer
