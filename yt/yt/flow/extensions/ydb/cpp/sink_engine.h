#pragma once

#include "public.h"

#include "attributes.h"
#include "options.h"
#include "state_access.h"

#include <yt/yt/flow/library/cpp/connectors/common/flow_queue_meta.h>

#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/sink_controller.h>

#include <yt/yt/core/concurrency/public.h>

#include <ydb-cpp-sdk/client/topic/deferred_publications.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TYdbMessageMeta PrepareYdbMessageMeta(
    const std::optional<std::string>& flowMetaName,
    const TFlowQueueMeta& flowMeta,
    const TYdbTopicAttributes& attributes);

////////////////////////////////////////////////////////////////////////////////

//! Writes one sink's output into its topic; owned by a schema-bound sink.
class TYdbSinkEngine
    : public TRefCounted
{
public:
    TYdbSinkEngine(
        TSinkContextPtr context,
        std::shared_ptr<const TYdbSinkOptions> options,
        IYdbClientFactoryPtr clientFactory,
        IStatusProfilerPtr statusProfiler,
        NLogging::TLogger logger);
    ~TYdbSinkEngine() override;

    const NLogging::TLogger& GetLogger() const;
    const IYdbClientFactoryPtr& GetClientFactory() const;
    const std::shared_ptr<const TYdbSinkOptions>& GetOptions() const;

    //! Throws unless every input stream has the payload column |sinkTypeName| distributes.
    void ValidatePayloadColumn(const std::string& sinkTypeName) const;

    void InitSession(const std::string& producerId);
    void InitSession(const std::string& producerId, const TYdbWriteTarget& target);
    void ResetSession();

    TFuture<void> Write(
        i64 seqNo,
        std::string&& data,
        TYdbMessageMeta&& messageMeta,
        std::optional<NYdb::NTopic::TDeferredPublication>&& deferredPublication = std::nullopt);

    TFuture<void> WriteMessage(
        i64 seqNo,
        const TOutputMessage& message,
        std::optional<NYdb::NTopic::TDeferredPublication> deferredPublication = std::nullopt);
    TFuture<void> WriteFramingBatch(
        i64 seqNo,
        const std::vector<TOutputMessageConstPtr>& messages,
        std::optional<NYdb::NTopic::TDeferredPublication> deferredPublication = std::nullopt);

    std::string ReadPayload(const TOutputMessage& message) const;
    std::string PackFramingBatch(const std::vector<TOutputMessageConstPtr>& messages) const;
    std::vector<std::pair<std::string, std::vector<TOutputMessageConstPtr>>> PackFramingBatches(
        const std::deque<TOutputMessageConstPtr>& messages,
        const TYdbBatchingOptions& batching) const;
    TYdbTopicAttributes ExtractAttributes(const TMessage& message) const;
    TYdbMessageMeta PrepareMessageMeta(
        const TFlowQueueMeta& flowMeta,
        const TYdbTopicAttributes& attributes) const;

    //! Starts a session under a fresh producer, so that sequence numbers restart from one.
    void InitAtLeastOnceSession();
    //! Writes every message, or every framed batch when |framingBatching| is set, and waits for all writes.
    void DistributeAtLeastOnce(
        const std::deque<TOutputMessageConstPtr>& messages,
        const TYdbBatchingOptions* framingBatching);

private:
    const TSinkContextPtr Context_;
    const std::shared_ptr<const TYdbSinkOptions> Options_;
    const IYdbClientFactoryPtr ClientFactory_;
    const IInvokerPtr Invoker_;
    const IStatusProfilerPtr StatusProfiler_;
    const NLogging::TLogger Logger;

    IYdbWriterPtr Writer_;
    i64 AtLeastOnceSeqNo_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TYdbSinkEngine);

////////////////////////////////////////////////////////////////////////////////

using TYdbWatermarkStateProvider = TCallback<TWatermarkStatePtr()>;

//! Discovers the sink's clusters and writes flow queue meta heartbeats into every partition.
class TYdbSinkControllerEngine
    : public TRefCounted
{
public:
    TYdbSinkControllerEngine(
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
        std::string topologyStatusPrefix);
    ~TYdbSinkControllerEngine() override;

    void Init(IInitContextPtr initContext);
    void Sync();
    void Commit();
    std::optional<i64> GetReceiverChannelCount();
    void Reconfigure(std::shared_ptr<const TYdbSinkControllerDynamicOptions> dynamicOptions);

    //! Federation as configured, or as last discovered when configuration does not tell.
    std::optional<bool> IsFederated() const;

private:
    const TSinkControllerContextPtr Context_;
    const TYdbTopologyOptions Topology_;
    const NLogging::TLogger Logger;
    const TYdbTopicInfoControllerPtr Info_;
    const TYdbStateAccess<TYdbHeartbeatSnapshot> HeartbeatState_;
    const bool WriteFlowQueueMeta_;
    const std::string FlowQueueMetaName_;
    const TYdbWatermarkStateProvider GetWatermarkState_;
    std::shared_ptr<const TYdbSinkControllerDynamicOptions> DynamicOptions_;

    IStatusErrorStatePtr WriteMetaErrorState_;
    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    THashMap<std::string, std::vector<TRetryableYdbWriterPtr>> Writers_;
    THashMap<std::string, std::vector<TFuture<void>>> Executors_;
    THashMap<std::string, std::vector<TFuture<void>>> WriteFutures_;
    i64 SeqNo_ = 0;

    void WriteFlowQueueMeta();
    void EnsureConnections(const THashMap<std::string, TYdbClusterSnapshot>& clusters);
};

DEFINE_REFCOUNTED_TYPE(TYdbSinkControllerEngine);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
