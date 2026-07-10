#include "distributed_chunk_session_sequencer.h"

#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/options.h>

#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NJournalClient;
using namespace NLogging;

using NApi::NNative::IConnectionPtr;
using NApi::NNative::TClientOptions;

namespace {

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
        , Logger(DistributedChunkSessionServiceLogger().WithTag("(SessionId: %v)", sessionId))
    { }

    TDistributedChunkSessionSequencer(
        TSessionId sessionId,
        IJournalChunkWriterPtr writer)
        : Writer_(std::move(writer))
        , Logger(DistributedChunkSessionServiceLogger().WithTag("(SessionId: %v)", sessionId))
    { }

    TFuture<void> Open() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto result = Writer_->Open();
        result.Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSequencer::OnWriterOpened,
            MakeWeak(this)));
        return result;
    }

    TFuture<void> GetClosedFuture() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ClosedPromise_.ToFuture().ToUncancelable();
    }

    TFuture<void> WriteRecord(TSharedRef record) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!IsOpen_) {
            return MakeFuture<void>(TError("Journal writer is not open"));
        }

        i64 recordIndex = RecordIndex_.fetch_add(1);
        YT_LOG_DEBUG("Writing record (RecordIndex: %v, RecordSize: %v)", recordIndex, record.Size());

        auto result = Writer_->WriteRecord(std::move(record)).AsVoid();
        result.Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSequencer::OnWriteFinished,
            MakeWeak(this),
            recordIndex));
        return result;
    }

    TFuture<void> Close() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!IsClosing_.exchange(true)) {
            ClosedPromise_.SetFrom(Writer_->Close());
        }

        return ClosedPromise_.ToFuture().ToUncancelable();
    }

private:
    const IJournalChunkWriterPtr Writer_;
    const TPromise<void> ClosedPromise_ = NewPromise<void>();
    const TLogger Logger;

    std::atomic<i64> RecordIndex_ = 0;

    std::atomic<bool> IsClosing_ = false;

    std::atomic<bool> IsOpen_ = false;

    void OnWriteFinished(i64 recordIndex, const TError& error)
    {
        if (error.IsOK()) {
            YT_LOG_DEBUG("Record writing finished (RecordIndex: %v)", recordIndex);
            return;
        }

        YT_LOG_DEBUG(error, "Record writing failed (RecordIndex: %v)", recordIndex);

        YT_UNUSED_FUTURE(Close());
    }

    void OnWriterOpened(const TError& error)
    {
        if (error.IsOK()) {
            YT_VERIFY(!IsOpen_.exchange(true));
            YT_LOG_DEBUG("Journal chunk writer was opened");
            return;
        }

        YT_LOG_DEBUG(error, "Failed to open journal chunk writer");

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
