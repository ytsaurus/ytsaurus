#include "source_engine.h"

#include "attributes.h"
#include "client_factory.h"
#include "helpers.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/tree_visitor.h>

#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <ydb-cpp-sdk/client/topic/control_plane.h>

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <util/generic/scope.h>

#include <limits>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr int YdbTopicDataColumnId = 0;
static constexpr int YdbTopicUnparsedColumnId = 1;
static constexpr int YdbTopicOffsetColumnId = 2;
static constexpr int YdbTopicSubOffsetColumnId = 3;
static constexpr int YdbTopicFirstOptionalColumnId = 4;

NTableClient::TTableSchemaPtr GetYdbTopicSourceSchema(
    const std::optional<std::string>& attributesColumn,
    const std::optional<std::string>& frameMetaColumn,
    const std::optional<std::string>& messageMetadataColumn,
    const std::optional<std::string>& writeTimeColumn)
{
    std::vector columns{
        TColumnSchema("data", EValueType::String),
        TColumnSchema("unparsed", EValueType::Any),
        TColumnSchema("offset", EValueType::Int64),
        TColumnSchema("sub_offset", EValueType::Int64),
    };
    const std::pair<const std::optional<std::string>*, EValueType> optionalColumns[] = {
        {&frameMetaColumn, EValueType::Any},
        {&attributesColumn, EValueType::Any},
        {&messageMetadataColumn, EValueType::Any},
        {&writeTimeColumn, EValueType::Uint64},
    };
    for (const auto& [optionalColumn, type] : optionalColumns) {
        if (!*optionalColumn) {
            continue;
        }
        for (const auto& column : columns) {
            if (column.Name() == **optionalColumn) {
                THROW_ERROR_EXCEPTION("Column %Qv is already present in the topic source schema",
                    **optionalColumn);
            }
        }
        columns.emplace_back(**optionalColumn, type);
    }
    return New<NTableClient::TTableSchema>(std::move(columns));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

NYdb::NTopic::TPartitionStats DescribeYdbPartitionStats(
    const std::shared_ptr<NYdb::NTopic::TTopicClient>& topicClient,
    const std::string& topic,
    i64 partitionIndex,
    TDuration timeout)
{
    auto result = WaitFor(FromArcadiaFuture(topicClient->DescribePartition(
        topic,
        partitionIndex,
        NYdb::NTopic::TDescribePartitionSettings()
            .IncludeStats(true)
            .ClientTimeout(timeout))))
        .ValueOrThrow();
    if (!result.IsSuccess()) {
        THROW_ERROR_EXCEPTION("Failed to describe partition %v", partitionIndex)
            .With("answer", ToString(static_cast<const NYdb::TStatus&>(result)));
    }
    const auto& stats = result.GetPartitionDescription().GetPartition().GetPartitionStats();
    THROW_ERROR_EXCEPTION_UNLESS(stats, "Description of partition %v has no statistics", partitionIndex);
    return *stats;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbSourceReadSession::TYdbSourceReadSession(
    TSourceContextPtr context,
    std::shared_ptr<const TYdbSourceOptions> options,
    std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions,
    std::shared_ptr<NYdb::NTopic::TTopicClient> topicClient,
    std::string topic,
    i64 partitionIndex,
    i64 persistedOffsetExclusive,
    NLogging::TLogger logger,
    IStatusErrorStatePtr readErrorState)
    : Logger(std::move(logger))
    , Context_(std::move(context))
    , Options_(std::move(options))
    , DynamicOptions_(std::move(dynamicOptions))
    , ReadErrorState_(readErrorState)
    , TopicClient_(std::move(topicClient))
    , Topic_(std::move(topic))
    , PartitionIndex_(partitionIndex)
    , PersistedOffsetExclusive_(persistedOffsetExclusive)
{ }

void TYdbSourceReadSession::Init()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    YT_TLOG_INFO("Initializing topic source read session");

    Executor_ = BIND(&TYdbSourceReadSession::Execute, MakeWeak(this))
        .AsyncVia(Context_->SerializedInvoker)
        .Run();
}

bool TYdbSourceReadSession::IsSessionEstablished() const
{
    YT_VERIFY(NConcurrency::VerifySerializedInvokerAffinity(Context_->SerializedInvoker));

    return Session_ != nullptr;
}

bool TYdbSourceReadSession::HasSignalSince(TInstant instant) const
{
    YT_VERIFY(NConcurrency::VerifySerializedInvokerAffinity(Context_->SerializedInvoker));

    return LastSignalInstant_ > std::max(instant, SessionStartInstant_);
}

TDuration TYdbSourceReadSession::GetSilenceDuration() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    return TInstant::Now() - std::max(LastSignalInstant_, SessionStartInstant_);
}

void TYdbSourceReadSession::Reconfigure(std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    if (AreNodesEqual(DynamicOptions_->RestartToken, dynamicOptions->RestartToken)) {
        return;
    }
    DynamicOptions_ = std::move(dynamicOptions);
    Terminate();
    Init();
}

void TYdbSourceReadSession::Terminate()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    TForbidContextSwitchGuard guard;
    YT_TLOG_INFO("Terminating topic source read session");
    Executor_.Cancel(TError("Terminated"));
    Session_ = {};
}

std::pair<bool, std::optional<TYdbSourceReadSession::TCompressedMessage>> TYdbSourceReadSession::GetNextMessage(i64 nextOffset, std::optional<i64> offsetLimitExclusive)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    auto session = Session_;

    if (!session) {
        return {false, std::nullopt};
    }

    while (session->Buffered.empty()) {
        auto eventMaybe = session->Read->GetEvent(false);
        if (!eventMaybe) {
            break;
        }
        auto& event = *eventMaybe;

        TForbidContextSwitchGuard guard;
        if (auto* dataEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
            LastSignalInstant_ = TInstant::Now();
            auto dataEventHolder = std::make_shared<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(std::move(*dataEvent));
            for (auto& topicMessage : dataEventHolder->GetCompressedMessages()) {
                if (!session->HasRead) {
                    // If there has not been data event in read session until this moment
                    // topicMessage.GetOffset() received in data event is the first available offset in partition.
                    YT_TLOG_INFO("Update min offset as first readOffset in partition session")
                        .With("FirstReadOffset", topicMessage.GetOffset());
                    OffsetsUpdate_.Fire(topicMessage.GetOffset(), std::nullopt, std::nullopt);
                }
                session->HasRead = true;
                const i64 offset = topicMessage.GetOffset();
                session->ReceivedOffsetExclusive = std::max(session->ReceivedOffsetExclusive, offset + 1);
                if (offset < PersistedOffsetExclusive_) {
                    topicMessage.Commit();
                    continue;
                }
                session->InflightOffsets_.insert(offset);

                const auto writeTime = topicMessage.GetWriteTime();
                TCompressedMessage message = {
                    .Offset = offset,
                    .WriteTimestamp = TSystemTimestamp(writeTime.Seconds()),
                    .CreateTimestamp = TSystemTimestamp(topicMessage.GetCreateTime().Seconds()),
                    .WriteTimeMilliseconds = writeTime.MilliSeconds(),
                    .Meta = std::nullopt,
                    .Data = std::shared_ptr<const std::string>(dataEventHolder, &topicMessage.GetData()),
                    .Codec = topicMessage.GetCodec(),
                };

                if (Options_->TryParseFlowQueueMeta || Options_->AttributesColumn) {
                    // Reading the legacy name lets a pipeline be updated before the ones writing
                    // into its topic; a pipeline that picked its own name never meant it.
                    const bool acceptsLegacyName = Options_->FlowQueueMetaName == DefaultFlowQueueMetaName;

                    std::optional<std::string_view> flowMetaValue;
                    std::optional<std::string_view> legacyFlowMetaValue;
                    for (const auto& [field, value] : topicMessage.GetMessageMeta()->Fields) {
                        // Attributes flow owns are exposed through #TCompressedMessage::Meta,
                        // and the sink refuses to write them back, so they stay out of the column.
                        if (Options_->AttributesColumn &&
                            !IsReservedYdbTopicAttributeName(field) &&
                            (!Options_->TryParseFlowQueueMeta || field != Options_->FlowQueueMetaName))
                        {
                            message.Attributes.emplace(field, value);
                        }
                        if (!Options_->TryParseFlowQueueMeta) {
                            continue;
                        }
                        if (field == Options_->FlowQueueMetaName && !flowMetaValue) {
                            flowMetaValue = value;
                        } else if (acceptsLegacyName && field == LegacyFlowQueueMetaName && !legacyFlowMetaValue) {
                            legacyFlowMetaValue = value;
                        }
                    }
                    if (!flowMetaValue) {
                        flowMetaValue = legacyFlowMetaValue;
                    }
                    if (flowMetaValue) {
                        try {
                            message.Meta = ConvertTo<TFlowQueueMeta>(TYsonStringBuf(*flowMetaValue));
                        } catch (const std::exception& ex) {
                            if (Options_->IgnoreMalformedFlowQueueMeta) {
                                YT_TLOG_WARNING("Failed to parse flow queue meta")
                                    .With("Value", *flowMetaValue)
                                    .With(ex);
                            } else {
                                THROW_ERROR_EXCEPTION("Failed to parse flow queue meta from %Qv", *flowMetaValue)
                                    .With(ex);
                            }
                        }
                    }
                }
                if (message.Offset >= nextOffset) {
                    if (const auto& metadataColumn = Options_->MessageMetadataColumn) {
                        message.MessageMetadata = metadataColumn->Serialize(topicMessage);
                    }
                    session->Buffered.emplace_back(std::move(message));
                }
            }
        } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event)) {
            THROW_ERROR_EXCEPTION("Commit offset event %Qv should be processed by handler",
                NYdb::NTopic::DebugString(event));
        } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
            THROW_ERROR_EXCEPTION("Start event %Qv should be already processed",
                NYdb::NTopic::DebugString(event));
        } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
            THROW_ERROR_EXCEPTION("Stop partition session %Qv should be processed by handler",
                NYdb::NTopic::DebugString(event));
        } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>(&event)) {
            THROW_ERROR_EXCEPTION("Partition status event %Qv should be processed by handler",
                NYdb::NTopic::DebugString(event));
        } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
            THROW_ERROR_EXCEPTION("Partition closed event %Qv should be processed by handler",
                NYdb::NTopic::DebugString(event));
        } else if (auto* endEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
            LastSignalInstant_ = TInstant::Now();
            YT_TLOG_INFO("Partition session reached the end of an inactive partition")
                .With("Event", NYdb::NTopic::DebugString(*endEvent));
            session->Ended = true;
            session->UnconfirmedEnd = std::move(*endEvent);
            PartitionEnd_.Fire(std::max(session->StartEndOffset, session->ReceivedOffsetExclusive));
            TryConfirmPartitionEnd();
        } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
            return {false, std::nullopt};
        }
    }

    if (session->Buffered.empty()) {
        return {!session->Ended, std::nullopt};
    }

    auto& message = session->Buffered.front();
    YT_VERIFY(message.Offset >= nextOffset);
    if (offsetLimitExclusive && message.Offset >= offsetLimitExclusive) {
        return {false, std::nullopt};
    }

    auto result = std::pair<bool, std::optional<TCompressedMessage>>(true, std::move(message));
    session->Buffered.pop_front();

    return result;
}

TFuture<void> TYdbSourceReadSession::WaitForEvent(TDuration timeout)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    auto timeoutFuture = TDelayedExecutor::MakeDelayed(timeout);
    auto session = Session_;
    if (!session) {
        return timeoutFuture;
    }
    if (!session->EventFuture || session->EventFuture.IsSet()) {
        session->EventFuture = FromArcadiaFuture(session->Read->WaitEvent());
    }

    auto waitFuture = AnySet(
        std::vector{session->EventFuture, timeoutFuture},
        TFutureCombinerOptions{.CancelInputOnShortcut = false})
        .AsVoid();
    waitFuture.Subscribe(BIND([timeoutFuture] (const TError&) {
        timeoutFuture.Cancel(TError("Event wait completed"));
    }));
    return waitFuture;
}

void TYdbSourceReadSession::ReportPersistedOffset(i64 offset)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    PersistedOffsetExclusive_ = offset;

    if (Session_ && Session_->Partition) {
        NYdb::NTopic::TDeferredCommit deferred;
        while (!Session_->InflightOffsets_.empty() && *Session_->InflightOffsets_.begin() < PersistedOffsetExclusive_) {
            deferred.Add(Session_->Partition, *Session_->InflightOffsets_.begin());
            Session_->InflightOffsets_.erase(Session_->InflightOffsets_.begin());
        }
        deferred.Commit();
    }
}

void TYdbSourceReadSession::RequestStatus()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    if (Session_ && Session_->Partition) {
        Session_->Partition->RequestStatus();
    }
}

std::optional<std::string> TYdbSourceReadSession::GetReadSessionId() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    if (!Session_ || !Session_->Partition) {
        return std::nullopt;
    }
    const auto& readSessionId = Session_->Partition->GetReadSessionId();
    if (readSessionId.empty()) {
        return std::nullopt;
    }
    return readSessionId;
}

void TYdbSourceReadSession::Execute()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    YT_TLOG_INFO("Started topic source read session");

    auto finally = Finally([&] {
        YT_TLOG_INFO("Terminated topic source read session");
        Session_ = {};
    });

    while (true) {
        try {
            YT_TLOG_INFO("Connecting to topic");
            auto errorPromise = NewPromise<void>();

            auto session = InitSession(errorPromise);
            YT_TLOG_INFO("Established connection to topic");
            {
                TForbidContextSwitchGuard guard;
                Session_ = session;
                SessionStartInstant_ = TInstant::Now();
                // Connecting proves nothing on its own: a wedged partition accepts a session and then goes
                // quiet. It does prove health when YDB reports there is nothing left to read, since then no
                // further event is owed to us and only the status probe could speak.
                if (session->StartEndOffset <= PersistedOffsetExclusive_) {
                    ReadErrorState_->ClearError();
                }
            }
            WaitFor(errorPromise.ToFuture()).ThrowOnError();
        } catch (const std::exception& ex) {
            static constexpr auto Message = "Session received error"_sb;
            auto error = TError(Message)
                .With(ex);
            YT_TLOG_WARNING(Message)
                .With(ex);
            TForbidContextSwitchGuard guard;
            Session_ = {};
            ReadErrorState_->SetError(error);
        }
    }
}

std::shared_ptr<TYdbSourceReadSession::TSession> TYdbSourceReadSession::InitSession(TPromise<void> errorPromise)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    const TDuration startSessionTimeout = Options_->StartSessionTimeout;

    auto dynamicOptions = DynamicOptions_;

    auto readSession = WrapReadSessionForAsyncDestruction(
        TopicClient_->CreateReadSession(
            NYdb::NTopic::TReadSessionSettings()
                .Decompress(false)
                .ConsumerName(Options_->Consumer)
                .AutoPartitioningSupport(Options_->ReceivePartitionEnd)
                .MaxMemoryUsageBytes(Options_->MaxMemoryUsageBytes)
                .MaxLag(dynamicOptions->MaxTimeLag)
                .ReadFromTimestamp(dynamicOptions->ReadFromInstant)
                .AppendTopics(
                    NYdb::NTopic::TTopicReadSettings()
                        .Path(Topic_)
                        .AppendPartitionIds(PartitionIndex_))
                .EventHandlers(
                    NYdb::NTopic::TReadSessionSettings::TEventHandlers()
                        .HandlersExecutor(CreateThreadPoolExecutorAdapter(Context_->SerializedInvoker))
                        .PartitionSessionStatusHandler(BIND(&TThis::OnPartitionSessionStatus, MakeWeak(this)))
                        .PartitionSessionClosedHandler(BIND(&TThis::OnPartitionSessionClosed, MakeWeak(this), errorPromise))
                        .SessionClosedHandler(BIND(&TThis::OnSessionClosed, MakeWeak(this), errorPromise))
                        .CommitOffsetAcknowledgementHandler(BIND(&TThis::OnCommitOffsetAcknowledgement, MakeWeak(this)))
                        .StopPartitionSessionHandler(BIND(&TThis::OnStopPartitionSession, MakeWeak(this), errorPromise)))
                .DecompressionExecutor(CreateThreadPoolExecutorAdapter(Context_->PoolInvoker))
                .Log(NLogging::CreateArcadiaLogBackend(Logger))),
        Context_->GetStaticResource(YdbDriverDefaultResourceId));


    const auto startTime = TInstant::Now();
    while (true) {
        if (errorPromise.IsSet()) {
            errorPromise.GetOrCrash().ThrowOnError();
        }

        auto event = readSession->GetEvent(false);
        if (!event) {
            const auto now = TInstant::Now();
            auto alreadyWait = now - startTime;
            if (alreadyWait > startSessionTimeout) {
                THROW_ERROR_EXCEPTION("Failed to init session in time")
                    .With("already_wait", alreadyWait)
                    .With("start_session_timeout", startSessionTimeout)
                    .With("start_time", startTime)
                    .With("now", now);
            }
        } else if (auto* dataEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            // Start from last committed offset. Can't use larger offset here, because of heuristic of first available message in partition.
            dataEvent->Confirm(dataEvent->GetCommittedOffset(), dataEvent->GetCommittedOffset());
            YT_TLOG_INFO("Read session started, start offset is committed");
            auto session = std::make_shared<TSession>();
            session->Read = readSession;
            session->Partition = dataEvent->GetPartitionSession();
            session->StartEndOffset = dataEvent->GetEndOffset();
            return session;
        } else {
            THROW_ERROR_EXCEPTION("Unexpected event during init: %Qv",
                NYdb::NTopic::DebugString(*event));
        }
        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    }
}

void TYdbSourceReadSession::SetPartitionEndOffset(i64 endOffsetExclusive)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    PartitionEndOffsetExclusive_ = endOffsetExclusive;
    TryConfirmPartitionEnd();
}

void TYdbSourceReadSession::TryConfirmPartitionEnd()
{
    auto session = Session_;
    // The SDK releases the events of the child partitions once the end is confirmed and asks to commit
    // everything read from the partition before that.
    if (!session || !session->UnconfirmedEnd || !PartitionEndOffsetExclusive_ || !session->InflightOffsets_.empty() ||
        session->CommittedOffsetExclusive < std::max(*PartitionEndOffsetExclusive_, session->ReceivedOffsetExclusive))
    {
        return;
    }
    YT_TLOG_INFO("Confirming the end of the partition")
        .With("CommittedOffsetExclusive", session->CommittedOffsetExclusive);
    session->UnconfirmedEnd->Confirm();
    session->UnconfirmedEnd.reset();
}

void TYdbSourceReadSession::OnCommittedOffset(i64 committedOffsetExclusive)
{
    if (Session_) {
        Session_->CommittedOffsetExclusive = std::max(Session_->CommittedOffsetExclusive, committedOffsetExclusive);
        TryConfirmPartitionEnd();
    }
}

void TYdbSourceReadSession::OnPartitionSessionStatus(
    const NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent& event)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    LastSignalInstant_ = TInstant::Now();
    YT_TLOG_INFO("Received status event")
        .With("Event", NYdb::NTopic::DebugString(event));
    OffsetsUpdate_.Fire(
        event.GetCommittedOffset(),
        event.GetEndOffset(),
        Options_->StatusObservesIdleness ? LastSignalInstant_ : event.GetWriteTimeHighWatermark());
    OnCommittedOffset(event.GetCommittedOffset());
}

void TYdbSourceReadSession::OnPartitionSessionClosed(
    TPromise<void> errorPromise,
    const NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& event)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    auto error = TError("Partition session is closed")
        .With("event", NYdb::NTopic::DebugString(event));
    YT_TLOG_INFO("Received partition session closed event")
        .With(error);
    errorPromise.TrySet(error);
}

void TYdbSourceReadSession::OnSessionClosed(
    TPromise<void> errorPromise,
    const NYdb::NTopic::TSessionClosedEvent& event)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    auto error = TError("Session is closed")
        .With("event", ToString(static_cast<const NYdb::TStatus&>(event)));
    YT_TLOG_INFO("Received session closed event")
        .With(error);
    errorPromise.TrySet(error);
}

void TYdbSourceReadSession::OnCommitOffsetAcknowledgement(const NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent& event)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    LastSignalInstant_ = TInstant::Now();
    YT_TLOG_INFO("Received commit offset acknowledgement event")
        .With("Event", NYdb::NTopic::DebugString(event));
    OffsetsUpdate_.Fire(event.GetCommittedOffset(), std::nullopt, std::nullopt);
    OnCommittedOffset(event.GetCommittedOffset());
}

void TYdbSourceReadSession::OnStopPartitionSession(
    TPromise<void> errorPromise,
    const NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    auto error = TError("Partition session is stopped")
        .With("event", NYdb::NTopic::DebugString(event));
    YT_TLOG_INFO("Received stop partition session event")
        .With(error);
    errorPromise.TrySet(error);
}

////////////////////////////////////////////////////////////////////////////////

TYdbSourceEngine::TYdbSourceEngine(
    TSourceContextPtr context,
    std::shared_ptr<const TYdbSourceOptions> options,
    std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions,
    IYdbClientFactoryPtr clientFactory,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler,
    TYdbPartitionInfoUpdater updatePartitionInfo)
    : Context_(std::move(context))
    , Options_(std::move(options))
    , DynamicOptions_(std::move(dynamicOptions))
    , UpdatePartitionInfo_(std::move(updatePartitionInfo))
    , Schema_(GetYdbTopicSourceSchema(
        Options_->AttributesColumn,
        Options_->FrameMetaColumn,
        Options_->MessageMetadataColumn ? std::make_optional(Options_->MessageMetadataColumn->Name) : std::nullopt,
        Options_->WriteTimeColumn))
    , Database_(Options_->Topology.Database)
    , Endpoint_(UnpackYdbTopicKey(Context_->SourceKey).first)
    , Topic_(Options_->Topology.Topic)
    , PartitionIndex_(UnpackYdbTopicKey(Context_->SourceKey).second)
    , Logger(logger
            .WithTag("Endpoint", Endpoint_)
            .WithTag("Database", Database_)
            .WithTag("Topic", Topic_)
            .WithTag("PartitionIndex", PartitionIndex_)
            .WithTag("Consumer", Options_->Consumer))
    , TopicClient_(clientFactory->CreateTopicClient(
        Endpoint_,
        Database_))
    , MalformedMessagesCounter_(profiler.Counter("/malformed_messages"))
{ }

void TYdbSourceEngine::Init(IStatusErrorStatePtr readErrorState, IStatusErrorStatePtr sessionLivenessErrorState)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    // Not an availability voter: a failing commit says nothing about whether reads work.
    UpdatePartitionInfoErrorState_ = Context_->StatusProfiler->ErrorState("update_partition_info");
    ReadErrorState_ = std::move(readErrorState);
    SessionLivenessErrorState_ = std::move(sessionLivenessErrorState);
    Initialized_ = true;
    if (!ReadBlocked_) {
        StartReadSession();
    }

    PartitionInfoUpdater_ = New<NConcurrency::TPeriodicExecutor>(
        Context_->SerializedInvoker,
        BIND(&TYdbSourceEngine::TryUpdatePartitionInfo, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(Options_->UpdateInfoPeriod));
    PartitionInfoUpdater_->Start();
}

void TYdbSourceEngine::StartReadSession()
{
    ReadSession_ = New<TYdbSourceReadSession>(
        Context_,
        Options_,
        DynamicOptions_,
        TopicClient_,
        Topic_,
        PartitionIndex_,
        PersistedOffsetExclusive_,
        Logger,
        ReadErrorState_);

    ReadSession_->SubscribeOffsetsUpdate(BIND(&TYdbSourceEngine::UpdatePartitionOffsets, MakeWeak(this)));
    ReadSession_->SubscribePartitionEnd(BIND(&TYdbSourceEngine::OnPartitionEnd, MakeWeak(this)));
    ReadSession_->Init();
}

void TYdbSourceEngine::StopReadSession()
{
    if (ReadSession_) {
        ReadSession_->Terminate();
        ReadSession_ = {};
    }
}

void TYdbSourceEngine::SetReadBlocked(bool readBlocked)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (ReadBlocked_ == readBlocked) {
        return;
    }
    ReadBlocked_ = readBlocked;
    YT_TLOG_INFO("Changed whether reading is blocked")
        .With("ReadBlocked", readBlocked);
    if (!Initialized_) {
        return;
    }
    if (readBlocked) {
        StopReadSession();
    } else if (!EndOffsetExclusive_) {
        StartReadSession();
    }
}

std::optional<i64> TYdbSourceEngine::GetEndOffsetExclusive() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    return EndOffsetExclusive_;
}

bool TYdbSourceEngine::IsCommitBehindPartitionEnd() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    return EndOffsetExclusive_ && CommittedOffsetExclusive_ < *EndOffsetExclusive_;
}

void TYdbSourceEngine::OnPartitionEnd(i64 receivedEndOffsetExclusive)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    ReceivedEndOffsetExclusive_ = std::max(ReceivedEndOffsetExclusive_.value_or(0), receivedEndOffsetExclusive);
    PartitionInfoUpdater_->ScheduleOutOfBand();
}

void TYdbSourceEngine::ConfirmPartitionEnd()
{
    auto stats = DescribeYdbPartitionStats(TopicClient_, Topic_, PartitionIndex_, Options_->Topology.ClientTimeout);
    const auto endOffsetExclusive = std::max(*ReceivedEndOffsetExclusive_, static_cast<i64>(stats.GetEndOffset()));
    YT_TLOG_INFO("Partition got no more writes")
        .With("EndOffsetExclusive", endOffsetExclusive)
        .With("RetainedStartOffset", stats.GetStartOffset());
    EndOffsetExclusive_ = endOffsetExclusive;
    if (ReadSession_) {
        ReadSession_->SetPartitionEndOffset(endOffsetExclusive);
    }
    UpdatePartitionInfo_(TYdbPartitionInfoUpdate{
        .CommittedOffsetExclusive = static_cast<i64>(stats.GetStartOffset()),
        .MaxOffsetExclusive = endOffsetExclusive,
        .UpdateInstant = TInstant::Now(),
    });
}

void TYdbSourceEngine::Reconfigure(std::shared_ptr<const TYdbSourceDynamicOptions> dynamicOptions)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    DynamicOptions_ = dynamicOptions;
    if (ReadSession_) {
        ReadSession_->Reconfigure(std::move(dynamicOptions));
    }
}

void TYdbSourceEngine::Terminate()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (PartitionInfoUpdater_) {
        YT_UNUSED_FUTURE(PartitionInfoUpdater_->Stop());
    }
    StopReadSession();
}

std::pair<std::vector<TPayload>, TTableSchemaPtr> TYdbSourceEngine::ProcessMessage(
    const TYdbSourceReadSession::TCompressedMessage& message)
{
    auto attributesYson = Options_->AttributesColumn
        ? ConvertToYsonString(message.Attributes)
        : TYsonString();
    const bool hasFrameMetaColumn = Options_->FrameMetaColumn.has_value();
    const bool hasMessageMetadataColumn = Options_->MessageMetadataColumn.has_value();
    const bool hasWriteTimeColumn = Options_->WriteTimeColumn.has_value();
    const int frameMetaColumnId = YdbTopicFirstOptionalColumnId;
    const int attributesColumnId = frameMetaColumnId + hasFrameMetaColumn;
    const int messageMetadataColumnId = attributesColumnId + Options_->AttributesColumn.has_value();
    const int writeTimeColumnId = messageMetadataColumnId + hasMessageMetadataColumn;
    auto setOptionalColumns = [&] (TMutableUnversionedRow row) {
        if (attributesYson) {
            row[attributesColumnId] = MakeUnversionedAnyValue(attributesYson.AsStringBuf(), attributesColumnId);
        }
        if (hasMessageMetadataColumn) {
            row[messageMetadataColumnId] = message.MessageMetadata
                ? MakeUnversionedAnyValue(message.MessageMetadata.AsStringBuf(), messageMetadataColumnId)
                : MakeUnversionedNullValue(messageMetadataColumnId);
        }
        if (hasWriteTimeColumn) {
            row[writeTimeColumnId] = MakeUnversionedUint64Value(message.WriteTimeMilliseconds, writeTimeColumnId);
        }
    };
    const i64 optionalColumnsSize = (attributesYson ? std::ssize(attributesYson.AsStringBuf()) : 0) +
        (message.MessageMetadata ? std::ssize(message.MessageMetadata.AsStringBuf()) : 0);

    try {
        auto data = message.Data;
        if (message.Codec != NYdb::NTopic::ECodec::RAW) {
            data = std::make_shared<std::string>(DecompressYdbTopicMessage(*data, message.Codec));
        }
        auto unpacked = Options_->SplitPayload(TSharedRef(TRef::FromString(*data), MakeSharedRangeHolder(data)));

        std::vector<TPayload> payloads;
        for (int i = 0; i < std::ssize(unpacked); ++i) {
            const auto& [payload, frameMeta] = unpacked[i];
            const bool writeFrameMeta = hasFrameMetaColumn && frameMeta;
            const i64 frameMetaSize = writeFrameMeta ? std::ssize(frameMeta.AsStringBuf()) : 0;
            payloads.push_back(TPayload(TPayload::TUnderlying(
                Schema_->GetColumnCount(),
                payload.Size() + frameMetaSize + optionalColumnsSize,
                [&] (TMutableUnversionedRow row) {
                    row[YdbTopicDataColumnId] = MakeUnversionedStringValue(payload.ToStringBuf(), YdbTopicDataColumnId);
                    row[YdbTopicUnparsedColumnId] = MakeUnversionedNullValue(YdbTopicUnparsedColumnId);
                    row[YdbTopicOffsetColumnId] = MakeUnversionedInt64Value(message.Offset, YdbTopicOffsetColumnId);
                    row[YdbTopicSubOffsetColumnId] = MakeUnversionedInt64Value(i, YdbTopicSubOffsetColumnId);
                    if (hasFrameMetaColumn) {
                        row[frameMetaColumnId] = writeFrameMeta
                            ? MakeUnversionedAnyValue(frameMeta.AsStringBuf(), frameMetaColumnId)
                            : MakeUnversionedNullValue(frameMetaColumnId);
                    }
                    setOptionalColumns(row);
                })));
        }
        return {std::move(payloads), Schema_};
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Failed to process topic message")
            .With("Offset", message.Offset)
            .With(ex);
        MalformedMessagesCounter_.Increment();
        switch (AppliedMalformedMessagePolicy_.value_or(DynamicOptions_->MalformedMessagePolicy)) {
            case EMalformedYdbMessagePolicy::Keep: {
                const auto unparsedYson = Options_->SerializeUnparsed(*message.Data);
                auto payload = TPayload(TPayload::TUnderlying(
                    Schema_->GetColumnCount(),
                    unparsedYson.AsStringBuf().size() + optionalColumnsSize,
                    [&] (TMutableUnversionedRow row) {
                        row[YdbTopicDataColumnId] = MakeUnversionedNullValue(YdbTopicDataColumnId);
                        row[YdbTopicUnparsedColumnId] = MakeUnversionedAnyValue(unparsedYson.AsStringBuf(), YdbTopicUnparsedColumnId);
                        row[YdbTopicOffsetColumnId] = MakeUnversionedInt64Value(message.Offset, YdbTopicOffsetColumnId);
                        row[YdbTopicSubOffsetColumnId] = MakeUnversionedInt64Value(0, YdbTopicSubOffsetColumnId);
                        if (hasFrameMetaColumn) {
                            row[frameMetaColumnId] = MakeUnversionedNullValue(frameMetaColumnId);
                        }
                        setOptionalColumns(row);
                    }));
                return {{std::move(payload)}, Schema_};
            }
            case EMalformedYdbMessagePolicy::Drop:
                break;
            case EMalformedYdbMessagePolicy::Fail:
                THROW_ERROR_EXCEPTION("Failed to process topic message")
                    .With(ex)
                    .With("endpoint", Endpoint_)
                    .With("topic", Topic_)
                    .With("partition_index", PartitionIndex_)
                    .With("offset", message.Offset);
        }
    }
    return {{}, Schema_};
}

void TYdbSourceEngine::SetAppliedMalformedMessagePolicy(EMalformedYdbMessagePolicy policy)
{
    AppliedMalformedMessagePolicy_ = policy;
}

EMalformedYdbMessagePolicy TYdbSourceEngine::GetMalformedMessagePolicy() const
{
    return DynamicOptions_->MalformedMessagePolicy;
}

TFuture<std::vector<TOrderedSourceBase::TRecord>> TYdbSourceEngine::ReadNextBatch(
    const TMessageBatcherSettingsPtr& settings,
    i64 nextOffset,
    std::optional<i64> offsetLimitExclusive)
{
    return ReadRecords(settings, nextOffset, offsetLimitExclusive, settings->MaxRowsPerBatch, settings->MaxBytesPerBatch);
}

TFuture<std::vector<TOrderedSourceBase::TRecord>> TYdbSourceEngine::ReadRange(
    const TMessageBatcherSettingsPtr& settings,
    i64 nextOffset,
    i64 endExclusive)
{
    return ReadRecords(settings, nextOffset, endExclusive, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());
}

TFuture<std::optional<i64>> TYdbSourceEngine::FetchTrimmedOffset()
{
    return BIND([topicClient = TopicClient_, topic = Topic_, partitionIndex = PartitionIndex_, timeout = Options_->Topology.ClientTimeout] {
        return std::optional<i64>(DescribeYdbPartitionStats(topicClient, topic, partitionIndex, timeout).GetStartOffset());
    })
        .AsyncVia(Context_->SerializedInvoker)
        .Run();
}

TFuture<std::vector<TOrderedSourceBase::TRecord>> TYdbSourceEngine::ReadRecords(
    const TMessageBatcherSettingsPtr& settings,
    i64 nextOffset,
    std::optional<i64> offsetLimitExclusive,
    i64 maxRowsPerBatch,
    i64 maxBytesPerBatch)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    const auto now = TInstant::Now();
    const auto deadline = LastBatchStartInstant_.value_or(now) + settings->BatchDuration;
    i64 byteSize = 0;
    std::vector<TOrderedSourceBase::TRecord> records;
    // The session of a fully read partition stays open: commits and statuses still go through it.
    auto isEndRead = [&] {
        return EndOffsetExclusive_ && nextOffset >= *EndOffsetExclusive_;
    };
    try {
        if (!ReadSession_ || isEndRead()) {
            WaitFor(TDelayedExecutor::MakeDelayed(deadline - now)).ThrowOnError();
        }
        while (ReadSession_ && !isEndRead() && byteSize < maxBytesPerBatch && std::ssize(records) < maxRowsPerBatch) {
            auto [canContinueReading, topicMessage] = ReadSession_->GetNextMessage(nextOffset, offsetLimitExclusive);
            if (!canContinueReading) {
                if (ReceivedEndOffsetExclusive_) {
                    WaitFor(TDelayedExecutor::MakeDelayed(deadline - TInstant::Now())).ThrowOnError();
                }
                break;
            }
            if (!topicMessage) {
                const auto remaining = deadline - TInstant::Now();
                if (remaining <= TDuration::Zero() ||
                    (offsetLimitExclusive && nextOffset >= *offsetLimitExclusive))
                {
                    break;
                }
                WaitFor(ReadSession_->WaitForEvent(remaining)).ThrowOnError();
                continue;
            }

            auto [payloads, payloadSchema] = ProcessMessage(*topicMessage);

            TOrderedSourceBase::TRecord record = {
                .Offset = IntToOffset(topicMessage->Offset),
                .WriteTimestamp = topicMessage->WriteTimestamp,
                .CreateTimestamp = topicMessage->CreateTimestamp,
                .Meta = std::move(topicMessage->Meta),
                .Payloads = std::move(payloads),
                .PayloadSchema = std::move(payloadSchema),
                .WriteTimeMilliseconds = topicMessage->WriteTimeMilliseconds,
            };
            for (const auto& payload : record.Payloads) {
                byteSize += payload.Underlying().GetSpaceUsed() + sizeof(record);
            }
            records.push_back(std::move(record));
            nextOffset = topicMessage->Offset + 1;
        }
        LastBatchStartInstant_ = TInstant::Now();
        return MakeFuture(std::move(records));
    } catch (const std::exception& ex) {
        static constexpr auto Message = "Critical read failure"_sb;
        YT_TLOG_ERROR(Message)
            .With(ex);
        return MakeFuture<std::vector<TOrderedSourceBase::TRecord>>(TError(Message)
                .With(ex));
    }
}

void TYdbSourceEngine::ReportPersistedOffset(i64 offset)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (offset != PersistedOffsetExclusive_) {
        PersistedOffsetChangeInstant_ = TInstant::Now();
    }
    PersistedOffsetExclusive_ = offset;

    if (ReadSession_) {
        ReadSession_->ReportPersistedOffset(offset);
    }
}

void TYdbSourceEngine::UpdatePartitionOffsets(
    i64 committedOffsetExclusive,
    std::optional<i64> maxOffsetExclusive,
    std::optional<TInstant> instant)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    CommittedOffsetExclusive_ = std::max(CommittedOffsetExclusive_, committedOffsetExclusive);
    if (committedOffsetExclusive >= PersistedOffsetExclusive_) {
        PersistedOffsetChangeInstant_ = TInstant::Zero();
    }
    UpdatePartitionInfo_(TYdbPartitionInfoUpdate{
        .CommittedOffsetExclusive = committedOffsetExclusive,
        .MaxOffsetExclusive = maxOffsetExclusive,
        .UpdateInstant = instant,
    });
}

void TYdbSourceEngine::DoHardCommit(i64 offset, const std::string& readSessionId)
{
    auto commitResult = NConcurrency::WaitFor(
        FromArcadiaFuture(
            TopicClient_->CommitOffset(
                Topic_,
                PartitionIndex_,
                Options_->Consumer,
                offset,
                NYdb::NTopic::TCommitOffsetSettings().ReadSessionId(readSessionId))))
        .ValueOrThrow();
    bool success = commitResult.IsSuccess();
    if (!success && commitResult.GetIssues().Size() == 1) {
        if (commitResult.GetIssues().begin()->GetCode() == YdbCommitToPastIssueCode) {
            success = true;
        }
    }
    THROW_ERROR_EXCEPTION_UNLESS(success, "Failed to commit offset: %v", ToString(static_cast<const NYdb::TStatus&>(commitResult)))
        .With("offset", offset)
        .With("read_session_id", readSessionId);
}

// An established session can stop delivering without ever reporting an error: no event is raised, so
// nothing would mark the partition broken while it produces nothing. The status request issued on every
// tick is the only round trip that proves otherwise, and its answer is the only thing that clears the
// error — a reconnect must not, or a session that wedges again immediately would look healthy forever.
void TYdbSourceEngine::EnsureReadSessionLiveness()
{
    YT_VERIFY(NConcurrency::VerifySerializedInvokerAffinity(Context_->SerializedInvoker));

    // Establishing a session is bounded by StartSessionTimeout and reports its own errors; the watchdog
    // only covers a session that connected and then went quiet.
    if (!ReadSession_ || !ReadSession_->IsSessionEstablished()) {
        return;
    }

    const bool heardFromTopic = ReadSession_->HasSignalSince(LastLivenessCheckInstant_);
    LastLivenessCheckInstant_ = TInstant::Now();
    if (heardFromTopic) {
        SessionLivenessErrorState_->ClearError();
        ReadErrorState_->ClearError();
        return;
    }

    // The probe is the only thing that refreshes the signal on an idle partition, so the budget has to
    // outlast several probe periods (which carry jitter) or every idle partition trips the watchdog on
    // every tick and its whole cluster ends up declared unavailable.
    const auto silenceTimeout = std::max(
        DynamicOptions_->SessionSilenceTimeout,
        Options_->UpdateInfoPeriod * 3);
    const auto silenceDuration = ReadSession_->GetSilenceDuration();
    if (silenceDuration <= silenceTimeout) {
        return;
    }

    auto error = TError("Read session delivered nothing for too long")
        .With("silence_duration", silenceDuration)
        .With("session_silence_timeout", silenceTimeout);
    YT_TLOG_WARNING("Recreating a silent read session")
        .With(error);
    SessionLivenessErrorState_->SetError(error);

    ReadSession_->Terminate();
    ReadSession_->Init();
}

void TYdbSourceEngine::TryUpdatePartitionInfo()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);

    static const std::string componentName = "UpdatePartitionInfo";
    try {
        const auto startInstant = TInstant::Now();

        if (ReadSession_) {
            YT_TLOG_INFO("Requesting status");
            ReadSession_->RequestStatus(); // Just request, do not wait for result here.
        }
        EnsureReadSessionLiveness();

        YT_TLOG_INFO("Consider hard commit")
            .With("PersistedOffsetExclusive", PersistedOffsetExclusive_)
            .With("PersistedOffsetChangeInstant", PersistedOffsetChangeInstant_)
            .With("Timeout", DynamicOptions_->CommitTimeout);

        // If no commit result have been received in specified time, do commit in a hard way.
        if (PersistedOffsetChangeInstant_ != TInstant::Zero() && PersistedOffsetChangeInstant_ + DynamicOptions_->CommitTimeout < TInstant::Now()) {
            auto readSessionId = ReadSession_ ? ReadSession_->GetReadSessionId() : std::nullopt;
            if (readSessionId) {
                const i64 persistedOffsetExclusive = PersistedOffsetExclusive_;
                const auto persistedOffsetChangeInstant = PersistedOffsetChangeInstant_;
                DoHardCommit(persistedOffsetExclusive, *readSessionId);
                // The RPC can yield while a newer persisted offset is reported.
                if (PersistedOffsetExclusive_ == persistedOffsetExclusive &&
                    PersistedOffsetChangeInstant_ == persistedOffsetChangeInstant)
                {
                    PersistedOffsetChangeInstant_ = TInstant::Zero();
                }
                YT_TLOG_INFO("Performed hard commit of persisted offset")
                    .With("CommittedOffsetExclusive", persistedOffsetExclusive)
                    .With("ReadSessionId", *readSessionId);
                CommittedOffsetExclusive_ = std::max(CommittedOffsetExclusive_, persistedOffsetExclusive);
                UpdatePartitionInfo_(TYdbPartitionInfoUpdate{.CommittedOffsetExclusive = persistedOffsetExclusive, .UpdateInstant = startInstant});
            } else {
                YT_TLOG_INFO("Deferring hard commit until a read session is established")
                    .With("PersistedOffsetExclusive", PersistedOffsetExclusive_);
            }
        }

        if (ReceivedEndOffsetExclusive_ && !EndOffsetExclusive_) {
            ConfirmPartitionEnd();
        }

        UpdatePartitionInfoErrorState_->ClearError();
    } catch (const std::exception& ex) {
        static constexpr auto Message = "Failed to update partition info"_sb;
        YT_TLOG_ERROR(Message)
            .With(ex);
        UpdatePartitionInfoErrorState_->SetError(TError(Message)
                .With(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
