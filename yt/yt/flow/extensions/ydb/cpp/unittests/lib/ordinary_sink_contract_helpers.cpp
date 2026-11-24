#include "ordinary_sink_contract_helpers.h"

#include <yt/yt/flow/library/cpp/common/sink.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NOrdinarySinkContract {
namespace {

template <class TResponse, class TResult>
void SetSuccessfulOperation(TResponse* response, const TResult& result)
{
    auto* operation = response->mutable_operation();
    operation->set_ready(true);
    operation->set_status(Ydb::StatusIds::SUCCESS);
    operation->mutable_result()->PackFrom(result);
}

void FillPartitionInfo(Ydb::Topic::DescribeTopicResult::PartitionInfo* info, const TFakeTopicPartition& partition)
{
    info->set_partition_id(partition.Id);
    info->set_active(partition.Active);
    for (auto parentId : partition.ParentIds) {
        info->add_parent_partition_ids(parentId);
    }
    for (auto childId : partition.ChildIds) {
        info->add_child_partition_ids(childId);
    }
    auto* offsets = info->mutable_partition_stats()->mutable_partition_offsets();
    offsets->set_start(partition.StartOffset);
    offsets->set_end(partition.EndOffset);
}

template <class TResponse>
void SetFailedOperation(TResponse* response, Ydb::StatusIds::StatusCode status, const std::string& message)
{
    auto* operation = response->mutable_operation();
    operation->set_ready(true);
    operation->set_status(status);
    operation->add_issues()->set_message(message);
}

} // namespace

TFakeTopicServer::TFakeTopicServer()
    : TopicService_(this)
    , DiscoveryService_(this)
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &Port_);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 1000);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
    builder.RegisterService(&TopicService_);
    builder.RegisterService(&DiscoveryService_);
    Server_ = builder.BuildAndStart();
    YT_VERIFY(Server_);
}

TFakeTopicServer::~TFakeTopicServer()
{
    Server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
    Server_->Wait();
}

std::string TFakeTopicServer::GetEndpoint() const
{
    return Format("localhost:%v", Port_);
}

void TFakeTopicServer::SetPartitionCount(int partitionCount)
{
    auto guard = std::lock_guard(Mutex_);
    PartitionCount_ = partitionCount;
}

void TFakeTopicServer::HoldAcknowledgements()
{
    auto guard = std::lock_guard(Mutex_);
    AcknowledgementsHeld_ = true;
}

void TFakeTopicServer::RejectSessionAfterWrites(int writeCount)
{
    auto guard = std::lock_guard(Mutex_);
    RejectAfterWriteCount_ = writeCount;
}

void TFakeTopicServer::Acknowledge(i64 seqNo)
{
    auto guard = std::lock_guard(Mutex_);
    YT_VERIFY(ActiveStream_);
    WriteAcknowledgement(ActiveStream_, seqNo);
}

std::vector<TRecordedTopicSession> TFakeTopicServer::GetSessions() const
{
    auto guard = std::lock_guard(Mutex_);
    return Sessions_;
}

std::string TFakeTopicServer::FormatSessions() const
{
    TStringBuilder result;
    for (const auto& session : GetSessions()) {
        result.AppendFormat("{producer=%v;closed=%v;seq_nos=[", session.ProducerId, session.Closed);
        for (const auto& write : session.Writes) {
            result.AppendFormat("%v;", write.SeqNo);
        }
        result.AppendString("]}");
    }
    return result.Flush();
}

std::vector<TRecordedTopicWrite> TFakeTopicServer::GetWritesOfProducer(const std::string& producerId) const
{
    std::vector<TRecordedTopicWrite> writes;
    for (const auto& session : GetSessions()) {
        if (session.ProducerId == producerId) {
            writes.insert(writes.end(), session.Writes.begin(), session.Writes.end());
        }
    }
    return writes;
}

void TFakeTopicServer::SetPartitions(std::vector<TFakeTopicPartition> listed, std::vector<TFakeTopicPartition> describedAlone)
{
    auto guard = std::lock_guard(Mutex_);
    ListedPartitions_ = std::move(listed);
    DescribedAlonePartitions_ = std::move(describedAlone);
}

void TFakeTopicServer::SetPartitionDescriptionsAvailable(bool available)
{
    auto guard = std::lock_guard(Mutex_);
    PartitionDescriptionsAvailable_ = available;
}

void TFakeTopicServer::SetTopicDescriptionsAvailable(bool available)
{
    auto guard = std::lock_guard(Mutex_);
    TopicDescriptionsAvailable_ = available;
}

int TFakeTopicServer::GetTopicDescriptionCount() const
{
    auto guard = std::lock_guard(Mutex_);
    return TopicDescriptionCount_;
}

int TFakeTopicServer::GetPartitionCount() const
{
    auto guard = std::lock_guard(Mutex_);
    return PartitionCount_;
}

void TFakeTopicServer::WriteAcknowledgement(TWriteStream* stream, i64 seqNo)
{
    TFromServer response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    auto* acknowledgement = response.mutable_write_response()->add_acks();
    acknowledgement->set_seq_no(seqNo);
    acknowledgement->mutable_written()->set_offset(seqNo);
    stream->Write(response);
}

grpc::Status TFakeTopicServer::ServeWriteStream(TWriteStream* stream)
{
    std::optional<size_t> sessionIndex;
    TFromClient request;
    while (stream->Read(&request)) {
        auto guard = std::lock_guard(Mutex_);
        if (request.has_init_request()) {
            const auto& init = request.init_request();
            sessionIndex = Sessions_.size();
            Sessions_.push_back({
                .Path = std::string(init.path()),
                .ProducerId = std::string(init.producer_id()),
                .MessageGroupId = std::string(init.message_group_id()),
            });
            ActiveStream_ = stream;

            TFromServer response;
            response.set_status(Ydb::StatusIds::SUCCESS);
            auto* initResponse = response.mutable_init_response();
            initResponse->set_session_id(Format("session-%v", *sessionIndex));
            initResponse->mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
            stream->Write(response);
        } else if (request.has_write_request()) {
            YT_VERIFY(sessionIndex);
            auto& session = Sessions_[*sessionIndex];
            for (const auto& message : request.write_request().messages()) {
                TYdbMessageMeta messageMeta;
                for (const auto& item : message.metadata_items()) {
                    messageMeta.emplace_back(std::string(item.key()), std::string(item.value()));
                }
                session.Writes.push_back({
                    .SeqNo = message.seq_no(),
                    .Data = std::string(message.data()),
                    .UncompressedSize = message.uncompressed_size(),
                    .MessageMeta = std::move(messageMeta),
                });
                if (!AcknowledgementsHeld_) {
                    WriteAcknowledgement(stream, message.seq_no());
                }
            }
            if (RejectAfterWriteCount_ && std::ssize(session.Writes) >= *RejectAfterWriteCount_) {
                RejectAfterWriteCount_.reset();
                TFromServer response;
                response.set_status(Ydb::StatusIds::BAD_REQUEST);
                stream->Write(response);
                break;
            }
        }
    }

    auto guard = std::lock_guard(Mutex_);
    if (sessionIndex) {
        Sessions_[*sessionIndex].Closed = true;
    }
    if (ActiveStream_ == stream) {
        ActiveStream_ = nullptr;
    }
    return grpc::Status::OK;
}

TFakeTopicServer::TTopicService::TTopicService(TFakeTopicServer* owner)
    : Owner_(owner)
{ }

grpc::Status TFakeTopicServer::TTopicService::StreamWrite(grpc::ServerContext* /*context*/, TWriteStream* stream)
{
    return Owner_->ServeWriteStream(stream);
}

grpc::Status TFakeTopicServer::TTopicService::DescribeTopic(
    grpc::ServerContext* /*context*/,
    const Ydb::Topic::DescribeTopicRequest* /*request*/,
    Ydb::Topic::DescribeTopicResponse* response)
{
    auto guard = std::lock_guard(Owner_->Mutex_);
    ++Owner_->TopicDescriptionCount_;
    if (!Owner_->TopicDescriptionsAvailable_) {
        SetFailedOperation(response, Ydb::StatusIds::UNAVAILABLE, "Topic descriptions are unavailable");
        return grpc::Status::OK;
    }
    Ydb::Topic::DescribeTopicResult result;
    if (Owner_->ListedPartitions_) {
        for (const auto& partition : *Owner_->ListedPartitions_) {
            FillPartitionInfo(result.add_partitions(), partition);
        }
    } else {
        for (int partitionId = 0; partitionId < Owner_->PartitionCount_; ++partitionId) {
            auto* partition = result.add_partitions();
            partition->set_partition_id(partitionId);
            partition->set_active(true);
        }
    }
    SetSuccessfulOperation(response, result);
    return grpc::Status::OK;
}

grpc::Status TFakeTopicServer::TTopicService::DescribePartition(
    grpc::ServerContext* /*context*/,
    const Ydb::Topic::DescribePartitionRequest* request,
    Ydb::Topic::DescribePartitionResponse* response)
{
    auto guard = std::lock_guard(Owner_->Mutex_);
    if (!Owner_->PartitionDescriptionsAvailable_) {
        SetFailedOperation(response, Ydb::StatusIds::UNAVAILABLE, "Partition descriptions are unavailable");
        return grpc::Status::OK;
    }
    Ydb::Topic::DescribePartitionResult result;
    auto* partition = result.mutable_partition();
    if (Owner_->ListedPartitions_) {
        const TFakeTopicPartition* found = nullptr;
        for (const auto* partitions : {&*Owner_->ListedPartitions_, &Owner_->DescribedAlonePartitions_}) {
            for (const auto& candidate : *partitions) {
                if (candidate.Id == request->partition_id()) {
                    found = &candidate;
                }
            }
        }
        if (!found) {
            SetFailedOperation(response, Ydb::StatusIds::BAD_REQUEST, Format("No partition %v in topic", request->partition_id()));
            return grpc::Status::OK;
        }
        FillPartitionInfo(partition, *found);
    } else {
        partition->set_partition_id(request->partition_id());
        partition->set_active(true);
    }
    partition->mutable_partition_location()->set_node_id(FakeNodeId);
    partition->mutable_partition_location()->set_generation(1);
    SetSuccessfulOperation(response, result);
    return grpc::Status::OK;
}

TFakeTopicServer::TDiscoveryService::TDiscoveryService(TFakeTopicServer* owner)
    : Owner_(owner)
{ }

grpc::Status TFakeTopicServer::TDiscoveryService::ListEndpoints(
    grpc::ServerContext* /*context*/,
    const Ydb::Discovery::ListEndpointsRequest* /*request*/,
    Ydb::Discovery::ListEndpointsResponse* response)
{
    Ydb::Discovery::ListEndpointsResult result;
    auto* endpoint = result.add_endpoints();
    endpoint->set_address("localhost");
    endpoint->set_port(Owner_->Port_);
    endpoint->set_node_id(FakeNodeId);
    SetSuccessfulOperation(response, result);
    return grpc::Status::OK;
}

TTopicServerResource::TTopicServerResource(
    TResourceContextPtr context,
    TDynamicResourceContextPtr dynamicContext,
    const std::string& endpoint,
    const std::string& database)
    : TResourceBase(std::move(context), std::move(dynamicContext))
    , Core_(New<TYdbDriverCore>(
        NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database),
        /*stopBeforeDestroying*/ true))
{ }

std::shared_ptr<NYdb::NTopic::TTopicClient> TTopicServerResource::CreateTopicClient(
    const std::string& endpoint,
    const std::string& database)
{
    return Core_->CreateTopicClient(endpoint, database);
}

std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> TTopicServerResource::CreateFederatedTopicClient(
    const std::string& endpoint,
    const std::string& database)
{
    THROW_ERROR_EXCEPTION_UNLESS(DiscoveryAvailable.load(), "Discovery is unavailable");
    return Core_->CreateFederatedTopicClient(endpoint, database);
}

IYdbWriterPtr TTopicServerResource::CreateWriter(
    const std::string& /*endpoint*/,
    const std::string& /*database*/,
    NYdb::NTopic::TWriteSessionSettings /*settings*/,
    IInvokerPtr /*invoker*/,
    NLogging::TLogger /*logger*/,
    IStatusProfilerPtr /*statusProfiler*/)
{
    YT_ABORT();
}

IDeferredPublicationClientPtr TTopicServerResource::CreateDeferredPublicationClient(
    const std::string& /*endpoint*/,
    const std::string& /*database*/)
{
    YT_ABORT();
}

TFuture<TYdbWriteTargetResolution> TTopicServerResource::ResolveWriteTarget(
    const TYdbTopologyOptions& /*options*/,
    TDuration /*timeout*/,
    std::optional<TYdbWriteTarget> /*preferredTarget*/,
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> /*excludedTargets*/)
{
    YT_ABORT();
}

TTopicServerResourcePtr CreateTopicServerResource(const std::string& resourceClassName, const std::string& endpoint)
{
    auto context = New<TResourceContext>();
    context->ResourceSpec = New<TResourceSpec>();
    context->ResourceSpec->ResourceClassName = resourceClassName;
    context->Logger = TestLogger;
    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = New<TDynamicResourceSpec>();
    return New<TTopicServerResource>(std::move(context), std::move(dynamicContext), endpoint, "/Root");
}

TFuture<ITimeProvider::TGlobalUniqueSeqNo> TControlledTimeProvider::GenerateGlobalUniqueSeqNo() const
{
    YT_ABORT();
}

i64 TControlledTimeProvider::GenerateSeqNo()
{
    YT_ABORT();
}

TFuture<void> TControlledTimeProvider::InsertSeqNoBarrier()
{
    YT_ABORT();
}

TFuture<TSystemTimestamp> TControlledTimeProvider::GetTimestamp(bool /*barrier*/) const
{
    ++TimestampCallCount;
    return MakeFuture(TSystemTimestamp(Now.load()));
}

IInitContextPtr TFailingInitContext::WithPrefix(TStringBuf /*prefix*/) const
{
    return New<TFailingInitContext>();
}

const std::string& TFailingInitContext::GetPrefix() const
{
    return Prefix_;
}

TFuture<IMutableStateProviderPtr> TFailingInitContext::CreateMutableStateProvider(std::function<IStateHolderPtr()> /*ctor*/) const
{
    return MakeFuture<IMutableStateProviderPtr>(TError("State storage is unavailable"));
}

void AddSinkParameter(const TSinkSpecPtr& spec, const std::string& name, const NYTree::INodePtr& value)
{
    spec->Parameters->RemoveChild(name);
    spec->Parameters->AddChild(name, value);
}

TDynamicSinkContextPtr MakeDynamicSinkContext(const std::string& name, const NYTree::INodePtr& value)
{
    auto context = New<TDynamicSinkContext>();
    context->DynamicSinkSpec = New<TDynamicSinkSpec>();
    context->DynamicSinkSpec->Parameters->AddChild(name, value);
    return context;
}

void AddAttributesColumn(TSinkTestEnvironment* environment, NTableClient::EValueType columnType)
{
    environment->Schema = New<NTableClient::TTableSchema>(std::vector{
        NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        NTableClient::TColumnSchema("attributes", columnType),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = environment->Schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[SinkTestStreamId][TStreamSpecId(1)] = streamSpec;
    environment->StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        New<NTableClient::TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    environment->Context->StreamSpecStorage = environment->StreamSpecStorage;
    environment->Context->SinkSpec->Parameters->AddChild("attributes_column", NYTree::ConvertToNode("attributes"));
}

TOutputMessageConstPtr MakeAttributedMessage(
    const TSinkTestEnvironment& environment,
    i64 id,
    const TYdbTopicAttributes& attributes)
{
    TMessageBuilder builder(SinkTestStreamId, environment.Schema);
    builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
    builder.SetSystemTimestamp(TSystemTimestamp(1'700'000'000));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1'700'000'000));
    builder.SetEventTimestamp(TSystemTimestamp(1'700'000'000));
    builder.Payload().Set<std::string>("payload", "payload");
    builder.Payload().Set<NYson::TYsonString>(NYson::ConvertToYsonString(attributes), "attributes");
    return New<TOutputMessage>(builder.Finish(), environment.StreamSpecStorage);
}

TFlowQueueMeta ParseFlowQueueMeta(const std::string& value)
{
    return NYTree::ConvertTo<TFlowQueueMeta>(NYson::TYsonStringBuf(TStringBuf(value)));
}

std::vector<i64> GetSeqNos(const std::vector<TYdbMessageToWrite>& writes)
{
    std::vector<i64> seqNos;
    for (const auto& write : writes) {
        seqNos.push_back(write.SeqNo);
    }
    return seqNos;
}

} // namespace NYT::NFlow::NOrdinarySinkContract
