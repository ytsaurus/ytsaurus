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
        return OKFuture;
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

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Failed);

    void SetClosed()
    {
        ClosePromise_.Set();
    }

    void SetWriteError(TError error)
    {
        WritePromise_.Set(std::move(error));
    }

private:
    const TPromise<void> ClosePromise_ = NewPromise<void>();
    const TPromise<i64> WritePromise_ = NewPromise<i64>();

    std::vector<TSharedRef> Records_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributedChunkSessionSequencerTest, PendingWriteErrorIsPropagatedAfterSequencerClose)
{
    auto writer = New<TControlledJournalChunkWriter>();
    auto sequencer = CreateDistributedChunkSessionSequencerForTesting(TSessionId(), writer);

    WaitFor(sequencer->Open())
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
