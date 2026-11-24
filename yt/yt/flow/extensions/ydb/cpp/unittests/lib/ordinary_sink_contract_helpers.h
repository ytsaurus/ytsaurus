#pragma once

#include "sink_test_environment.h"

#include <yt/yt/flow/extensions/ydb/cpp/driver_core.h>

#include <yt/yt/flow/library/cpp/connectors/common/flow_queue_meta.h>

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <contrib/ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <contrib/ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <grpcpp/grpcpp.h>

#include <mutex>

namespace NYT::NFlow::NOrdinarySinkContract {

using namespace NYdbTesting;

using TFromClient = Ydb::Topic::StreamWriteMessage::FromClient;
using TFromServer = Ydb::Topic::StreamWriteMessage::FromServer;

struct TRecordedTopicWrite
{
    i64 SeqNo = 0;
    std::string Data;
    i64 UncompressedSize = 0;
    TYdbMessageMeta MessageMeta;
};

struct TRecordedTopicSession
{
    std::string Path;
    std::string ProducerId;
    std::string MessageGroupId;
    bool Closed = false;
    std::vector<TRecordedTopicWrite> Writes;
};

struct TFakeTopicPartition
{
    i64 Id = 0;
    bool Active = true;
    std::vector<i64> ParentIds = {};
    std::vector<i64> ChildIds = {};
    i64 StartOffset = 0;
    i64 EndOffset = 0;
};

class TFakeTopicServer
{
public:
    TFakeTopicServer();
    ~TFakeTopicServer();

    std::string GetEndpoint() const;

    void SetPartitionCount(int partitionCount);
    //! Replaces the partition count: |listed| make the topic description, while |describedAlone| are found
    //! only by describing the partition itself. Describing any other partition reports it missing.
    void SetPartitions(std::vector<TFakeTopicPartition> listed, std::vector<TFakeTopicPartition> describedAlone = {});
    void SetPartitionDescriptionsAvailable(bool available);
    void SetTopicDescriptionsAvailable(bool available);
    int GetTopicDescriptionCount() const;
    void HoldAcknowledgements();
    void RejectSessionAfterWrites(int writeCount);
    void Acknowledge(i64 seqNo);

    std::vector<TRecordedTopicSession> GetSessions() const;
    std::string FormatSessions() const;
    std::vector<TRecordedTopicWrite> GetWritesOfProducer(const std::string& producerId) const;

private:
    using TWriteStream = grpc::ServerReaderWriter<TFromServer, TFromClient>;

    class TTopicService
        : public Ydb::Topic::V1::TopicService::Service
    {
    public:
        explicit TTopicService(TFakeTopicServer* owner);

        grpc::Status StreamWrite(grpc::ServerContext* context, TWriteStream* stream) override;

        grpc::Status DescribeTopic(
            grpc::ServerContext* context,
            const Ydb::Topic::DescribeTopicRequest* request,
            Ydb::Topic::DescribeTopicResponse* response) override;

        grpc::Status DescribePartition(
            grpc::ServerContext* context,
            const Ydb::Topic::DescribePartitionRequest* request,
            Ydb::Topic::DescribePartitionResponse* response) override;

    private:
        TFakeTopicServer* const Owner_;
    };

    class TDiscoveryService
        : public Ydb::Discovery::V1::DiscoveryService::Service
    {
    public:
        explicit TDiscoveryService(TFakeTopicServer* owner);

        grpc::Status ListEndpoints(
            grpc::ServerContext* context,
            const Ydb::Discovery::ListEndpointsRequest* request,
            Ydb::Discovery::ListEndpointsResponse* response) override;

    private:
        TFakeTopicServer* const Owner_;
    };

    static constexpr int FakeNodeId = 1;

    mutable std::mutex Mutex_;
    int PartitionCount_ = 1;
    std::optional<std::vector<TFakeTopicPartition>> ListedPartitions_;
    std::vector<TFakeTopicPartition> DescribedAlonePartitions_;
    bool PartitionDescriptionsAvailable_ = true;
    bool TopicDescriptionsAvailable_ = true;
    int TopicDescriptionCount_ = 0;
    bool AcknowledgementsHeld_ = false;
    std::optional<int> RejectAfterWriteCount_;
    std::vector<TRecordedTopicSession> Sessions_;
    TWriteStream* ActiveStream_ = nullptr;

    int Port_ = 0;
    TTopicService TopicService_;
    TDiscoveryService DiscoveryService_;
    std::unique_ptr<grpc::Server> Server_;

    int GetPartitionCount() const;
    grpc::Status ServeWriteStream(TWriteStream* stream);
    static void WriteAcknowledgement(TWriteStream* stream, i64 seqNo);
};

class TTopicServerResource
    : public TResourceBase
    , public IYdbClientFactory
{
public:
    std::atomic<bool> DiscoveryAvailable = true;

    TTopicServerResource(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext,
        const std::string& endpoint,
        const std::string& database);

    std::shared_ptr<NYdb::NTopic::TTopicClient> CreateTopicClient(
        const std::string& endpoint,
        const std::string& database) override;

    std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> CreateFederatedTopicClient(
        const std::string& endpoint,
        const std::string& database) override;

    IYdbWriterPtr CreateWriter(
        const std::string& endpoint,
        const std::string& database,
        NYdb::NTopic::TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler) override;

    IDeferredPublicationClientPtr CreateDeferredPublicationClient(
        const std::string& endpoint,
        const std::string& database) override;

    TFuture<TYdbWriteTargetResolution> ResolveWriteTarget(
        const TYdbTopologyOptions& options,
        TDuration timeout,
        std::optional<TYdbWriteTarget> preferredTarget,
        THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets) override;

private:
    const TYdbDriverCorePtr Core_;
};

using TTopicServerResourcePtr = TIntrusivePtr<TTopicServerResource>;

TTopicServerResourcePtr CreateTopicServerResource(const std::string& resourceClassName, const std::string& endpoint);

class TControlledTimeProvider
    : public ITimeProvider
{
public:
    std::atomic<ui64> Now = 1;
    mutable std::atomic<int> TimestampCallCount = 0;

    TFuture<TGlobalUniqueSeqNo> GenerateGlobalUniqueSeqNo() const override;
    i64 GenerateSeqNo() override;
    TFuture<void> InsertSeqNoBarrier() override;
    TFuture<TSystemTimestamp> GetTimestamp(bool barrier) const override;
};

class TFailingInitContext
    : public IInitContext
{
public:
    IInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;
    TFuture<IMutableStateProviderPtr> CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const override;

private:
    const std::string Prefix_;
};

void AddSinkParameter(const TSinkSpecPtr& spec, const std::string& name, const NYTree::INodePtr& value);

TDynamicSinkContextPtr MakeDynamicSinkContext(const std::string& name, const NYTree::INodePtr& value);

void AddAttributesColumn(TSinkTestEnvironment* environment, NTableClient::EValueType columnType);

TOutputMessageConstPtr MakeAttributedMessage(
    const TSinkTestEnvironment& environment,
    i64 id,
    const TYdbTopicAttributes& attributes);

TFlowQueueMeta ParseFlowQueueMeta(const std::string& value);

std::vector<i64> GetSeqNos(const std::vector<TYdbMessageToWrite>& writes);

} // namespace NYT::NFlow::NOrdinarySinkContract
