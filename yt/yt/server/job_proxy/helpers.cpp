#include "helpers.h"

#include "job.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/session_provider.h>
#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer.h>
#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer_adapter.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NControllerAgent::NProto;
using namespace NDistributedChunkSessionClient;
using namespace NNodeTrackerClient;
using namespace NPushBasedShuffleClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TOneShotFlag::Set(bool value) noexcept
{
    YT_VERIFY(!Flag_.has_value());
    Flag_ = value;
}

bool TOneShotFlag::Get() const noexcept
{
    YT_VERIFY(Flag_.has_value());
    return Flag_.value();
}

void TOneShotFlag::operator=(bool value) noexcept
{
    Set(value);
}

TOneShotFlag::operator bool () const noexcept
{
    return Get();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJobHostPartitionWriteSessionProvider
    : public IPartitionWriteSessionProvider
{
public:
    explicit TJobHostPartitionWriteSessionProvider(IJobHostPtr host)
        : Host_(std::move(host))
    { }

    TFuture<TSessionDescriptor> GetSession(
        int partitionIndex,
        std::optional<NChunkClient::TSessionId> excludedSessionId) final
    {
        return Host_->GetShuffleWriteSession(partitionIndex, excludedSessionId);
    }

private:
    const IJobHostPtr Host_;
};

////////////////////////////////////////////////////////////////////////////////

class TThreadOwningShuffleWriter
    : public IPushBasedShuffleWriter
{
public:
    TThreadOwningShuffleWriter(
        TActionQueuePtr writerQueue,
        IPushBasedShuffleWriterPtr underlyingWriter)
        : WriterQueue_(std::move(writerQueue))
        , UnderlyingWriter_(std::move(underlyingWriter))
    { }

    TFuture<void> Write(TRange<TUnversionedRow> rows) final
    {
        return UnderlyingWriter_->Write(rows);
    }

    TFuture<void> Close() final
    {
        return UnderlyingWriter_->Close();
    }

private:
    const TActionQueuePtr WriterQueue_;
    const IPushBasedShuffleWriterPtr UnderlyingWriter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateJobShuffleWriter(
    const IJobHostPtr& host,
    const TPartitionJobSpecExt& partitionJobSpecExt)
{
    const auto& writerSpec = partitionJobSpecExt.push_based_shuffle_writer();

    THashMap<int, TSessionDescriptor> seededSessions;
    seededSessions.reserve(writerSpec.seeded_sessions_size());
    for (const auto& protoSession : writerSpec.seeded_sessions()) {
        seededSessions.emplace(
            protoSession.partition_index(),
            TSessionDescriptor{
                .SessionId = FromProto<NChunkClient::TSessionId>(protoSession.session_id()),
                .SequencerNode = FromProto<TNodeDescriptor>(protoSession.sequencer_node()),
            });
    }

    auto writerQueue = New<TActionQueue>("ShuffleWriter");
    auto writer = CreatePushBasedShuffleWriter(
        ConvertTo<TShuffleWriterConfigPtr>(TYsonString(writerSpec.writer_config())),
        FromProto<NCompression::ECodec>(writerSpec.codec()),
        New<TJobHostPartitionWriteSessionProvider>(host),
        CreatePartitioner(partitionJobSpecExt),
        host->GetClient()->GetNativeConnection(),
        writerSpec.task_job_index(),
        writerQueue->GetInvoker(),
        std::move(seededSessions));

    TTableSchemaPtr schema;
    FromProto(&schema, writerSpec.intermediate_stream_schema());

    return CreateShuffleWriterAdapter(
        New<TThreadOwningShuffleWriter>(std::move(writerQueue), std::move(writer)),
        std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
