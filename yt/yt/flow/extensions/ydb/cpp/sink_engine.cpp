#include "sink_engine.h"

#include "client_factory.h"
#include "helpers.h"
#include "topic_info.h"
#include "writer.h"

#include <yt/yt/flow/extensions/framing/cpp/packer.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/guid.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NYson;
using namespace NYdb::NTopic;
using NConcurrency::WaitFor;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TPreparedMessage
{
    std::string Data;
    TYdbMessageMeta MessageMeta;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbMessageMeta PrepareYdbMessageMeta(
    const std::optional<std::string>& flowMetaName,
    const TFlowQueueMeta& flowMeta,
    const TYdbTopicAttributes& attributes)
{
    TYdbMessageMeta meta(attributes.begin(), attributes.end());
    if (flowMetaName) {
        meta.emplace_back(*flowMetaName, std::string{ConvertToYsonString(flowMeta).AsStringBuf()});
    }
    ValidateYdbMessageMeta(meta);
    return meta;
}

////////////////////////////////////////////////////////////////////////////////

TYdbSinkEngine::TYdbSinkEngine(
    TSinkContextPtr context,
    std::shared_ptr<const TYdbSinkOptions> options,
    IYdbClientFactoryPtr clientFactory,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger)
    : Context_(std::move(context))
    , Options_(std::move(options))
    , ClientFactory_(std::move(clientFactory))
    , Invoker_(Context_->SerializedInvoker)
    , StatusProfiler_(std::move(statusProfiler))
    , Logger(logger
            .WithTag("Endpoint", Options_->DisplayEndpoint)
            .WithTag("Database", Options_->Topology.Database)
            .WithTag("Topic", Options_->Topology.Topic))
{
    if (Options_->AttributesColumn) {
        for (const auto& streamId : Context_->SinkSpec->InputStreamIds) {
            auto schema = Context_->StreamSpecStorage->GetSchema(streamId);
            const auto* column = schema->FindColumn(*Options_->AttributesColumn);
            if (column == nullptr) {
                THROW_ERROR_EXCEPTION("Stream %Qv should have %Qv column to write topic message attributes",
                    streamId,
                    *Options_->AttributesColumn);
            }
            if (column->GetWireType() != EValueType::Any) {
                THROW_ERROR_EXCEPTION("Column %Qv of stream %Qv should have type any to hold topic message attributes",
                    *Options_->AttributesColumn,
                    streamId);
            }
        }
    }
}

TYdbSinkEngine::~TYdbSinkEngine() = default;

const NLogging::TLogger& TYdbSinkEngine::GetLogger() const
{
    return Logger;
}

const IYdbClientFactoryPtr& TYdbSinkEngine::GetClientFactory() const
{
    return ClientFactory_;
}

const std::shared_ptr<const TYdbSinkOptions>& TYdbSinkEngine::GetOptions() const
{
    return Options_;
}

void TYdbSinkEngine::ValidatePayloadColumn(const std::string& sinkTypeName) const
{
    for (const auto& streamId : Context_->SinkSpec->InputStreamIds) {
        auto schema = Context_->StreamSpecStorage->GetSchema(streamId);
        if (schema->FindColumn(Options_->PayloadColumn) == nullptr) {
            THROW_ERROR_EXCEPTION("Stream %Qv should have %Qv column to be distributed by %v",
                streamId,
                Options_->PayloadColumn,
                sinkTypeName);
        }
    }
}

void TYdbSinkEngine::InitSession(const std::string& producerId)
{
    InitSession(producerId, TYdbWriteTarget{.Endpoint = Options_->Endpoint, .Database = Options_->Topology.Database});
}

void TYdbSinkEngine::InitSession(const std::string& producerId, const TYdbWriteTarget& target)
{
    Writer_ = ClientFactory_->CreateWriter(
        target.Endpoint,
        target.Database,
        TWriteSessionSettings()
            .MaxMemoryUsage(Options_->MaxMemoryUsageBytes)
            .Path(Options_->Topology.Topic)
            .ProducerId(producerId)
            .MessageGroupId(producerId)
            .ValidateSeqNo(true)
            .CompressionExecutor(CreateThreadPoolExecutorAdapter(Invoker_))
            .Codec(ParseYdbCodec(Options_->Codec))
            .CompressionLevel(Options_->CompressionLevel),
        Invoker_,
        Logger.WithTag("Session", producerId),
        StatusProfiler_->WithPrefix("/session"));
    YT_VERIFY(Writer_);
}

void TYdbSinkEngine::ResetSession()
{
    Writer_.Reset();
}

TFuture<void> TYdbSinkEngine::Write(
    i64 seqNo,
    std::string&& data,
    TYdbMessageMeta&& messageMeta,
    std::optional<TDeferredPublication>&& deferredPublication)
{
    return Writer_->Write({
        .SeqNo = seqNo,
        .Data = std::move(data),
        .MessageMeta = std::move(messageMeta),
        .DeferredPublication = std::move(deferredPublication),
    });
}

std::string TYdbSinkEngine::ReadPayload(const TOutputMessage& message) const
{
    return GetColumnValue<std::string>(message, Options_->PayloadColumn);
}

std::string TYdbSinkEngine::PackFramingBatch(const std::vector<TOutputMessageConstPtr>& messages) const
{
    return PackBatch(messages, Options_->PayloadColumn, *Options_->FramingFormat);
}

std::vector<std::pair<std::string, std::vector<TOutputMessageConstPtr>>> TYdbSinkEngine::PackFramingBatches(
    const std::deque<TOutputMessageConstPtr>& messages,
    const TYdbBatchingOptions& batching) const
{
    return PackBatches(
        messages,
        Options_->PayloadColumn,
        *Options_->FramingFormat,
        batching.MaxRowsPerBatch,
        batching.MaxBytesPerBatch);
}

TYdbTopicAttributes TYdbSinkEngine::ExtractAttributes(const TMessage& message) const
{
    if (!Options_->AttributesColumn) {
        return {};
    }

    try {
        auto attributes = ParseYdbTopicAttributesColumn(
            GetColumnValue<std::optional<TYsonStringBuf>>(message, *Options_->AttributesColumn));
        if (Options_->WriteFlowQueueMeta && attributes.contains(Options_->FlowQueueMetaName)) {
            THROW_ERROR_EXCEPTION("Topic message attribute %Qv is reserved for the flow queue meta",
                Options_->FlowQueueMetaName);
        }
        return attributes;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to read topic message attributes from column %Qv",
            *Options_->AttributesColumn)
            .With(ex);
    }
}

TYdbMessageMeta TYdbSinkEngine::PrepareMessageMeta(
    const TFlowQueueMeta& flowMeta,
    const TYdbTopicAttributes& attributes) const
{
    auto flowMetaName = Options_->WriteFlowQueueMeta
        ? std::optional<std::string>{Options_->FlowQueueMetaName}
        : std::nullopt;
    return PrepareYdbMessageMeta(flowMetaName, flowMeta, attributes);
}

TFuture<void> TYdbSinkEngine::WriteMessage(
    i64 seqNo,
    const TOutputMessage& message,
    std::optional<TDeferredPublication> deferredPublication)
{
    return Write(
        seqNo,
        ReadPayload(message),
        PrepareMessageMeta(BuildFlowQueueMeta(message), ExtractAttributes(message)),
        std::move(deferredPublication));
}

TFuture<void> TYdbSinkEngine::WriteFramingBatch(
    i64 seqNo,
    const std::vector<TOutputMessageConstPtr>& messages,
    std::optional<TDeferredPublication> deferredPublication)
{
    if (messages.empty()) {
        return OKFuture;
    }
    return Write(
        seqNo,
        PackFramingBatch(messages),
        PrepareMessageMeta(BuildFlowQueueMeta(messages), {}),
        std::move(deferredPublication));
}

void TYdbSinkEngine::InitAtLeastOnceSession()
{
    auto producerId = ToString(TGuid::Create());
    InitSession(producerId);
}

void TYdbSinkEngine::DistributeAtLeastOnce(
    const std::deque<TOutputMessageConstPtr>& messages,
    const TYdbBatchingOptions* framingBatching)
{
    std::vector<TPreparedMessage> preparedMessages;
    if (framingBatching) {
        auto packed = PackFramingBatches(messages, *framingBatching);
        preparedMessages.reserve(packed.size());
        for (auto& [data, parents] : packed) {
            YT_VERIFY(!parents.empty());
            auto flowMeta = BuildFlowQueueMeta(parents);
            preparedMessages.push_back({
                .Data = std::move(data),
                .MessageMeta = PrepareMessageMeta(flowMeta, /*attributes*/ {}),
            });
        }
    } else {
        preparedMessages.reserve(messages.size());
        for (const auto& message : messages) {
            auto data = ReadPayload(*message);
            auto flowMeta = BuildFlowQueueMeta(*message);
            auto attributes = ExtractAttributes(*message);
            preparedMessages.push_back({
                .Data = std::move(data),
                .MessageMeta = PrepareMessageMeta(flowMeta, attributes),
            });
        }
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(preparedMessages.size());
    for (auto& message : preparedMessages) {
        AtLeastOnceSeqNo_ += 1;
        const auto seqNo = AtLeastOnceSeqNo_;
        auto future = Write(
            seqNo,
            std::move(message.Data),
            std::move(message.MessageMeta));
        futures.push_back(std::move(future));
    }
    for (auto& future : futures) {
        NConcurrency::WaitFor(future).ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TYdbSinkControllerEngine::TYdbSinkControllerEngine(
    TSinkControllerContextPtr context,
    TYdbTopologyOptions topology,
    TYdbStateAccess<TYdbTopologySnapshot> topologyState,
    TYdbStateAccess<TYdbHeartbeatSnapshot> heartbeatState,
    std::shared_ptr<const TYdbSinkControllerDynamicOptions> dynamicOptions,
    bool writeFlowQueueMeta,
    std::string flowQueueMetaName,
    IYdbClientFactoryPtr clientFactory,
    TYdbWatermarkStateProvider getWatermarkState,
    NLogging::TLogger logger,
    std::string topologyStatusPrefix)
    : Context_(std::move(context))
    , Topology_(std::move(topology))
    , Logger(std::move(logger))
    , Info_(New<TYdbTopicInfoController>(
        Topology_,
        std::move(clientFactory),
        Context_->Invoker,
        Logger,
        Context_->StatusProfiler->WithPrefix(topologyStatusPrefix),
        std::move(topologyState)))
    , HeartbeatState_(std::move(heartbeatState))
    , WriteFlowQueueMeta_(writeFlowQueueMeta)
    , FlowQueueMetaName_(std::move(flowQueueMetaName))
    , GetWatermarkState_(std::move(getWatermarkState))
    , DynamicOptions_(std::move(dynamicOptions))
    , WriteMetaErrorState_(Context_->StatusProfiler->ErrorState("/write_meta"))
    , HeartbeatExecutor_(New<NConcurrency::TPeriodicExecutor>(
        Context_->Invoker,
        BIND(&TYdbSinkControllerEngine::WriteFlowQueueMeta, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(DynamicOptions_->FlowQueueMetaHeartbeatPeriod)))
{ }

void TYdbSinkControllerEngine::Init(IInitContextPtr initContext)
{
    Info_->Init(initContext->WithPrefix("queue_info"));
    HeartbeatState_.Init(initContext);
    if (HeartbeatState_.Read().ProducerIdBase.empty()) {
        HeartbeatState_.Write({.ProducerIdBase = ToString(TGuid::Create())});
    }
    if (WriteFlowQueueMeta_) {
        HeartbeatExecutor_->Start();
    }
}

void TYdbSinkControllerEngine::Sync()
{
    Info_->Sync();
}

void TYdbSinkControllerEngine::Commit()
{
    Info_->Commit();
}

void TYdbSinkControllerEngine::Reconfigure(std::shared_ptr<const TYdbSinkControllerDynamicOptions> dynamicOptions)
{
    HeartbeatExecutor_->SetPeriod(dynamicOptions->FlowQueueMetaHeartbeatPeriod);
    DynamicOptions_ = std::move(dynamicOptions);
}

std::optional<bool> TYdbSinkControllerEngine::IsFederated() const
{
    return Topology_.KnownFederated ? Topology_.KnownFederated : Info_->IsFederated();
}

TYdbSinkControllerEngine::~TYdbSinkControllerEngine()
{
    // An empty cluster map tears every writer down without reading |Info_|'s state, which is unset when
    // Init() failed and would throw out of a destructor.
    EnsureConnections({});
}

std::optional<i64> TYdbSinkControllerEngine::GetReceiverChannelCount()
{
    std::optional<i64> totalCount;
    for (const auto& [endpoint, cluster] : Info_->GetClusters()) {
        if (cluster.PartitionCount) {
            totalCount = totalCount.value_or(0) + *cluster.PartitionCount;
        }
    }
    return totalCount;
}

void TYdbSinkControllerEngine::WriteFlowQueueMeta()
{
    try {
        auto watermarkState = GetWatermarkState_();
        if (!watermarkState) {
            YT_TLOG_WARNING("Watermark state is not available, skipping heartbeat");
            return;
        }

        TSystemTimestamp watermark = InfinitySystemTimestamp;
        for (const auto& streamId : Context_->SinkSpec->InputStreamIds) {
            watermark = std::min(watermark, watermarkState->GetEventWatermark(streamId));
        }
        TFlowQueueMeta meta;
        meta.EventWatermark = watermark;
        meta.PureHeartbeat = true;
        auto messageMeta = PrepareYdbMessageMeta(
            std::optional<std::string>{FlowQueueMetaName_},
            meta,
            /*attributes*/ {});
        YT_TLOG_INFO("Writing flow queue meta heartbeat")
            .With("EventWatermark", meta.EventWatermark);
        EnsureConnections(Info_->GetClusters());

        // The producer ids survive restarts, so the seqno must keep growing across them too;
        // the cluster clock guarantees that, and deduplication of a repeated seqno just drops
        // a heartbeat, which the next tick covers.
        auto now = WaitFor(Context_->TimeProvider->GetTimestamp(/*barrier*/ false))
            .ValueOrThrow();
        SeqNo_ = std::max<i64>(SeqNo_ + 1, now.Underlying());
        THashMap<std::string, THashSet<i64>> badPartitions;
        for (const auto& [endpoint, writers] : Writers_) {
            auto& futures = WriteFutures_[endpoint];
            YT_VERIFY(writers.size() == futures.size());

            for (i64 i = 0; i < std::ssize(writers); ++i) {
                if (futures[i].IsSet()) {
                    futures[i] = writers[i]->Write({
                        .SeqNo = SeqNo_,
                        .Data = "",
                        .MessageMeta = messageMeta,
                    });
                } else {
                    badPartitions[endpoint].insert(i);
                }
            }
        }
        if (badPartitions.size() > 0) {
            THashMap<std::string, i64> counts;
            for (const auto& [endpoint, partitions] : badPartitions) {
                counts[endpoint] = partitions.size();
            }
            YT_TLOG_WARNING("Failed to write flow queue meta heartbeat to some partitions")
                .With("CountPerEndpoint", ConvertToYsonString(counts, EYsonFormat::Text))
                .With("Partitions", ConvertToYsonString(badPartitions, EYsonFormat::Text));
        }
        WriteMetaErrorState_->ClearError();
    } catch (const std::exception& ex) {
        // TODO: pass to public controller log.
        static constexpr auto Message = "Failed to write flow queue meta heartbeat"_sb;
        WriteMetaErrorState_->SetError(TError(Message)
                .With(ex));
        YT_TLOG_WARNING(Message)
            .With(ex);
    }
}

void TYdbSinkControllerEngine::EnsureConnections(const THashMap<std::string, TYdbClusterSnapshot>& clusters)
{
    auto topicClients = Info_->GetTopicClients();

    THashMap<std::string, i64> targetCounts;
    for (const auto& [endpoint, cluster] : clusters) {
        targetCounts[endpoint] = cluster.PartitionCount.value_or(0);
    }
    for (const auto& endpoint : GetKeys(Writers_)) {
        if (!targetCounts.contains(endpoint)) {
            targetCounts[endpoint] = 0;
        }
    }

    for (const auto& [endpoint, count] : targetCounts) {
        auto& writers = Writers_[endpoint];
        auto& futures = WriteFutures_[endpoint];
        auto& executors = Executors_[endpoint];

        if (std::size(writers) > 0 && std::ssize(writers) != count) {
            YT_TLOG_INFO("Stopping heartbeat writers")
                .With("Endpoint", endpoint)
                .With("Count", std::ssize(writers));
            for (auto& future : executors) {
                future.Cancel(TError("Interrupted"));
            }
            writers = {};
            executors = {};
            futures = {};
        }

        if (std::ssize(writers) != count && count > 0) {
            auto clientIt = topicClients.find(endpoint);
            if (clientIt == topicClients.end()) {
                // The per-cluster client appears after the first successful discovery.
                YT_TLOG_INFO("Skipping heartbeat writers until cluster client is discovered")
                    .With("Endpoint", endpoint);
                continue;
            }

            const auto producerIdBase = HeartbeatState_.Read().ProducerIdBase;
            YT_TLOG_INFO("Creating heartbeat writers")
                .With("Endpoint", endpoint)
                .With("Count", count);

            for (i64 i = 0; i < count; ++i) {
                // Deterministic and persistent, so restarts and reconnections reuse the same
                // producers instead of littering the topic with new ones.
                auto producerId = Format("%v-%v", producerIdBase, i);
                auto writer = New<TRetryableYdbWriter>(
                    clientIt->second,
                    NYdb::NTopic::TWriteSessionSettings()
                        .MaxMemoryUsage(100_KB)
                        .Path(Info_->GetTopic())
                        .ProducerId(producerId)
                        .MessageGroupId(producerId)
                        .PartitionId(i)
                        .ValidateSeqNo(true)
                        .CompressionExecutor(CreateThreadPoolExecutorAdapter(Context_->Invoker)),
                    Logger
                        .WithTag("Endpoint", endpoint)
                        .WithTag("PartitionId", i),
                    Context_->StatusProfiler->WithPrefix("/writer"));
                executors.push_back(
                    BIND(&TRetryableYdbWriter::Run, writer)
                        .AsyncVia(Context_->Invoker)
                        .Run());
                writers.push_back(writer);
            }
            futures.assign(count, OKFuture);
            YT_VERIFY(futures.size() == writers.size());
            YT_VERIFY(executors.size() == writers.size());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
