#include "server.h"

#include "config.h"
#include "connection.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/client_cache.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/kafka/protocol.h>
#include <yt/yt/client/kafka/error.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>

namespace NYT::NKafkaProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NKafka;
using namespace NNet;
using namespace NThreading;
using namespace NQueueClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = KafkaProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        TKafkaProxyConfigPtr config,
        NNative::IConnectionPtr connection,
        IPollerPtr poller,
        IPollerPtr acceptor,
        IListenerPtr listener)
        : Config_(std::move(config))
        , Connection_(std::move(connection))
        , Poller_(std::move(poller))
        , Acceptor_(std::move(acceptor))
        , Listener_(std::move(listener))
    {
        RegisterTypedHandler(BIND(&TServer::DoApiVersions));
        RegisterTypedHandler(BIND(&TServer::DoMetadata, Connection_, Config_));
        RegisterTypedHandler(BIND(&TServer::DoFindCoordinator, Config_));
        RegisterTypedHandler(BIND(&TServer::DoJoinGroup));
        RegisterTypedHandler(BIND(&TServer::DoSyncGroup));
        RegisterTypedHandler(BIND(&TServer::DoHeartbeat));
        RegisterTypedHandler(BIND(&TServer::DoOffsetFetch, Connection_));
        RegisterTypedHandler(BIND(&TServer::DoFetch, Connection_));
        RegisterTypedHandler(BIND(&TServer::DoSaslHandshake));
        RegisterTypedHandler(BIND(&TServer::DoSaslAuthenticate, Connection_));
    }

    void Start() override
    {
        YT_VERIFY(!Started_);

        AsyncAcceptConnection();

        Started_ = true;

        YT_LOG_INFO("Kafka server started");
    }

    void RegisterHandler(ERequestType requestType, THandler handler) override
    {
        YT_VERIFY(!Handlers_[requestType]);
        Handlers_[requestType] = std::move(handler);
    }

private:
    std::atomic<bool> Started_ = false;

    const TKafkaProxyConfigPtr Config_;

    const NNative::IConnectionPtr Connection_;
    const NNative::TClientCachePtr AuthenticatedClientCache_;

    IPollerPtr Poller_;
    IPollerPtr Acceptor_;
    IListenerPtr Listener_;

    struct TConnectionState final
    {
        explicit TConnectionState(IConnectionPtr connection)
            : Connection(std::move(connection))
        { }

        IConnectionPtr Connection;

        std::optional<ERequestType> ExpectedNextRequestType = {ERequestType::SaslHandshake};
    };
    using TConnectionStatePtr = TIntrusivePtr<TConnectionState>;

    THashMap<TConnectionId, TConnectionStatePtr> Connections_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConnectionMapLock_);

    TEnumIndexedArray<ERequestType, THandler> Handlers_;

    void AsyncAcceptConnection()
    {
        Listener_->Accept().Subscribe(
            BIND(&TServer::OnConnectionAccepted, MakeWeak(this))
                .Via(Acceptor_->GetInvoker()));
    }

    void OnConnectionAccepted(const TErrorOr<NNet::IConnectionPtr>& connectionOrError)
    {
        AsyncAcceptConnection();

        if (!connectionOrError.IsOK()) {
            YT_LOG_INFO(connectionOrError, "Error accepting connection");
            return;
        }

        const auto& connection = connectionOrError.Value();
        auto kafkaConnection = CreateConnection(
            Config_,
            connection,
            Poller_->GetInvoker(),
            BIND(&TServer::OnRequest, MakeWeak(this)),
            BIND(&TServer::OnConnectionFailure, MakeWeak(this)));
        kafkaConnection->Start();

        YT_LOG_DEBUG("Connection accepted "
            "(ConnectionId: %v, LocalAddress: %v, RemoteAddress: %v)",
            kafkaConnection->GetConnectionId(),
            connection->LocalAddress(),
            connection->RemoteAddress());

        auto connectionState = New<TConnectionState>(kafkaConnection);
        EmplaceOrCrash(
            Connections_,
            kafkaConnection->GetConnectionId(),
            std::move(connectionState));
    }

    void OnRequest(IConnectionPtr connection, TMessage request)
    {
        try {
            GuardedOnRequest(connection, std::move(request));
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to process request "
                "(ConnectionId: %v)",
                connection->GetConnectionId());

            OnConnectionFailure(connection, TError(ex));
        }
    }

    void GuardedOnRequest(IConnectionPtr connection, TMessage request)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TConnectionStatePtr connectionState;
        {
            auto guard = ReaderGuard(ConnectionMapLock_);
            auto connectionIt = Connections_.find(connection->GetConnectionId());
            if (connectionIt == Connections_.end()) {
                THROW_ERROR_EXCEPTION("Connection %v is not registered",
                    connection->GetConnectionId());
            }
            connectionState = connectionIt->second;
        }

        auto response = ProcessRequest(connection, request, connectionState);

        YT_UNUSED_FUTURE(connection->PostMessage(response)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    OnConnectionFailure(connection, error);
                }
            })));
    }

    TMessage ProcessRequest(
        const IConnectionPtr& connection,
        const TMessage& request,
        const TConnectionStatePtr& connectionState)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (request.size() != 1) {
            THROW_ERROR_EXCEPTION("Incoming message has %v parts, expected 1",
                request.size());
        }

        auto expectedRequestType = connectionState->ExpectedNextRequestType;
        // For SaslHandshake v0 tokens are sent as opaque packets without wrapping the messages with Kafka protocol headers.
        if (expectedRequestType && *expectedRequestType == ERequestType::SaslAuthenticate) {
            // TODO(nadya73): check auth bytes and save username etc.
            connectionState->ExpectedNextRequestType = std::nullopt;
            return TSharedRefArrayBuilder(1).Finish();
        }

        auto reader = CreateKafkaProtocolReader(request[0]);

        TRequestHeader header;
        header.Deserialize(reader.get());

        YT_LOG_DEBUG("Request received (ApiKey: %v, ApiVersion: %v, CorrelationId: %v, ClientId: %v, ConnectionId: %v)",
            header.RequestType,
            header.ApiVersion,
            header.CorrelationId,
            header.ClientId,
            connection->GetConnectionId());

        // ApiVersions request could be sent before SaslHandshake, so let's allow it always.
        if (expectedRequestType && header.RequestType != *expectedRequestType && header.RequestType != ERequestType::ApiVersions) {
            THROW_ERROR_EXCEPTION("Incoming request is %v, but %v was expected", header.RequestType, *expectedRequestType);
        }

        auto responseHeader = [&header]() {
            auto protocolWriter = NKafka::CreateKafkaProtocolWriter();
            TResponseHeader result;
            result.CorrelationId = header.CorrelationId;
            result.Serialize(protocolWriter.get());
            return protocolWriter->Finish();
        }();

        if (header.RequestType == ERequestType::SaslHandshake) {
            connectionState->ExpectedNextRequestType = ERequestType::SaslAuthenticate;
        }

        if (auto handler = Handlers_[header.RequestType]; handler) {
            auto responseMessage = handler(reader.get(), header.ApiVersion);

            YT_LOG_DEBUG("Response sent (HeaderSize: %v, MessageSize: %v)", responseHeader.Size(), responseMessage.Size());

            TSharedRefArrayBuilder builder(3);
            builder.Add(responseHeader);
            builder.Add(responseMessage);

            return builder.Finish();
        } else {
            THROW_ERROR_EXCEPTION("Incoming message has invalid type, ignored (Type: %x)",
                    static_cast<int>(header.RequestType));
        }
    }

    void OnConnectionFailure(IConnectionPtr connection, TError error)
    {
        auto connectionId = connection->GetConnectionId();

        YT_LOG_INFO(error, "Kafka proxy server observed connection failure, terminating connection",
            "(ConnectionId: %v)",
            connectionId);

        if (UnregisterConnection(connection->GetConnectionId())) {
            // TODO(max42): switch to Subscribe.
            YT_UNUSED_FUTURE(connection->Terminate()
                .Apply(BIND([=] (const TError& error) {
                    YT_LOG_WARNING(error, "Failed to terminate connection");
                })));
        }
    }

    bool UnregisterConnection(TConnectionId connectionId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(ConnectionMapLock_);
        if (Connections_.erase(connectionId)) {
            YT_LOG_DEBUG("Connection unregistered (ConnectionId: %v)",
                connectionId);
            return true;
        }

        return false;
    }

    static TRspApiVersions DoApiVersions(const TReqApiVersions& request)
    {
        YT_LOG_DEBUG("Start to handle ApiVersions request (ClientSoftwareName: %Qv)", request.ClientSoftwareName);

        TRspApiVersions response;
        response.ApiKeys = std::vector<TRspApiKey>{
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::ApiVersions),
                .MinVersion = 0,
                .MaxVersion = 2,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::Metadata),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::Fetch),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::FindCoordinator),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            /*
            // TODO(nadya73): support it later.
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::JoinGroup),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::SyncGroup),
                .MinVersion = 0,
                .MaxVersion = 0,
            },*/
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::ListOffsets),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::OffsetCommit),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::OffsetFetch),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::Heartbeat),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::SaslHandshake),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::SaslAuthenticate),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            /*
            // TODO(nadya73): Support it later.
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::UpdateMetadata),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::DescribeGroups),
                .MinVersion = 0,
                .MaxVersion = 0,
            },*/
        };

        return response;
    }

    static TRspSaslHandshake DoSaslHandshake(const TReqSaslHandshake& request)
    {
        static const TString OAuthBearerSaslMechanism = "OAUTHBEARER";

        YT_LOG_DEBUG("Start to handle SaslHandshake request (Mechanism: %v, IsEqual: %v)", request.Mechanism, request.Mechanism == OAuthBearerSaslMechanism);

        TRspSaslHandshake response;
        response.Mechanisms = {OAuthBearerSaslMechanism};
        if (request.Mechanism != OAuthBearerSaslMechanism) {
            YT_LOG_DEBUG("Unsupported sasl mechanism (Requested: %v, Expected: %v)", request.Mechanism, OAuthBearerSaslMechanism);
            response.ErrorCode = NKafka::EErrorCode::UnsupportedSaslMechanism;
        }

        return response;
    }

    static TRspSaslAuthenticate DoSaslAuthenticate(NNative::IConnectionPtr /*connection*/, const TReqSaslAuthenticate& /*request*/)
    {
        TRspSaslAuthenticate response;

        return response;
    }

    static TRspMetadata DoMetadata(NNative::IConnectionPtr connection, const TKafkaProxyConfigPtr& config, const TReqMetadata& request)
    {
        YT_LOG_DEBUG("Start to handle Metadata request (TopicsSize: %v)", request.Topics.size());

        TRspMetadata response;
        response.Brokers = std::vector<TRspMetadataBroker>{
            TRspMetadataBroker{
                .NodeId = 0,
                .Host = "localhost",
                .Port = config->Port,
                .Rack = "1",
            },
        };

        response.Topics.reserve(request.Topics.size());
        for (const auto& topic : request.Topics) {
            auto path = TRichYPath::Parse(topic.Topic);
            auto tableInfo = WaitFor(connection->GetTableMountCache()->GetTableInfo(path.GetPath()))
                .ValueOrThrow();

            TRspMetadataTopic topicResponse{
                .Name = topic.Topic,
                .TopicId = topic.TopicId,
            };
            topicResponse.Partitions.reserve(tableInfo->Tablets.size());
            for (int tabletIndex = 0; tabletIndex < std::ssize(tableInfo->Tablets); ++tabletIndex) {
                topicResponse.Partitions.push_back(TRspMetadataTopicPartition{
                    .PartitionIndex = tabletIndex,
                    .LeaderId = 0,
                    .ReplicaNodes = {0},
                });
            }

            response.Topics.push_back(std::move(topicResponse));
        }

        return response;
    }

    static TRspFindCoordinator DoFindCoordinator(const TKafkaProxyConfigPtr& config, const TReqFindCoordinator& request)
    {
        YT_LOG_DEBUG("Start to handle FindCoordinator request (Key: %v)", request.Key);

        TRspFindCoordinator response;
        response.NodeId = 0;
        response.Host = "localhost";
        response.Port = config->Port;

        return response;
    }

    static TRspJoinGroup DoJoinGroup(const TReqJoinGroup& request)
    {
        YT_LOG_DEBUG("Start to handle JoinGroup request (GroupId: %Qv, MemberId: %Qv, ProtocolType: %Qv)", request.GroupId, request.MemberId, request.ProtocolType);

        TRspJoinGroup response;
        // TODO(nadya73): fill it with normal data.
        response.MemberId = request.MemberId;
        response.ProtocolName = "roundrobin";
        response.Leader = "leader_123";

        return response;
    }

    static TRspSyncGroup DoSyncGroup(const TReqSyncGroup& request)
    {
        YT_LOG_DEBUG("Start to handle SyncGroup request (GroupId: %v, MemberId: %v)", request.GroupId, request.MemberId);

        TRspSyncGroup response;
        TRspSyncGroupAssignment assignment;
        // TODO(nadya73): fill it with normal data.
        assignment.Topic = "primary://tmp/queue";
        assignment.Partitions = {0};
        response.Assignments.push_back(std::move(assignment));

        return response;
    }

    static TRspHeartbeat DoHeartbeat(const TReqHeartbeat& request)
    {
        YT_LOG_DEBUG("Start to handle Heartbreat request (GroupId: %v, MemberId: %v)", request.GroupId, request.MemberId);

        TRspHeartbeat response;

        return response;
    }

    static TRspOffsetFetch DoOffsetFetch(NNative::IConnectionPtr connection, const TReqOffsetFetch& request)
    {
        YT_LOG_DEBUG("Start to handle OffsetFetch request (GroupId: %v)", request.GroupId);

        TRspOffsetFetch response;
        response.Topics.reserve(request.Topics.size());

        // TODO(nadya73): use actual user after authorization support.
        auto client = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::RootUserName));
        YT_VERIFY(connection->GetClusterName());

        auto path = TRichYPath::Parse(request.GroupId);
        auto consumerClient = CreateConsumerClient(client, path.GetPath());

        for (const auto& topic : request.Topics) {
            TRspOffsetFetchTopic topicResponse{
                .Name = topic.Name,
            };
            topicResponse.Partitions.reserve(topic.PartitionIndexes.size());

            // TODO(nadya73): add CollectPartitions in IConsumerClient too.
            auto subConsumerClient = consumerClient->GetSubConsumerClient(client, TCrossClusterReference::FromString(topic.Name));
            auto partitions = WaitFor(subConsumerClient->CollectPartitions(topic.PartitionIndexes))
                .ValueOrThrow();
            for (auto partition : partitions) {
                topicResponse.Partitions.push_back(TRspOffsetFetchTopicPartition{
                    // TODO(nadya73): add type check.
                    .PartitionIndex = static_cast<int32_t>(partition.PartitionIndex),
                    .CommittedOffset = partition.NextRowIndex,
                    .ErrorCode = 0,
                });
            }

            response.Topics.push_back(std::move(topicResponse));
        }

        return response;
    }

    static TRspFetch DoFetch(NNative::IConnectionPtr connection, const TReqFetch& request)
    {
        YT_LOG_DEBUG("Start to handle Fetch request (TopicsSize: %v)", request.Topics.size());
        // TODO(nadya73): log requested offsets.

        TRspFetch response;
        response.Responses.reserve(request.Topics.size());

        auto client = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

        for (const auto& topic : request.Topics) {
            TRspFetchResponse topicResponse{
                .Topic = topic.Topic,
            };
            topicResponse.Partitions.reserve(topic.Partitions.size());

            for (const auto& partition : topic.Partitions) {
                TRspFetchResponsePartition topicPartitionResponse{
                    .PartitionIndex = partition.Partition,
                    .HighWatermark = 0, // TODO(nadya73): fill it with normal data
                };

                // TODO(nadya73): use PullConsumer.
                auto result = WaitFor(client->PullQueue(
                    topic.Topic,
                    partition.FetchOffset,
                    partition.Partition,
                    TQueueRowBatchReadOptions{.MaxDataWeight = partition.PartitionMaxBytes}))
                    .ValueOrThrow();

                auto rows = result->GetRows();

                YT_LOG_DEBUG("Rows were fetched (Count: %v)", rows.size());

                if (rows.size() > 0) {
                    topicPartitionResponse.Records = std::vector<TRecord>{};
                    topicPartitionResponse.Records->reserve(rows.size());

                    auto offset = result->GetStartOffset();

                    for (const auto& row : rows) {
                        topicPartitionResponse.Records->push_back(TRecord{
                            .Offset = offset,
                            .Message = {
                                NKafka::TMessage{
                                    .Key = "",
                                    // TODO(nadya73): convert it to yson/json.
                                    .Value = ToString(row),
                                }
                            }
                        });
                        ++offset;
                    }
                }

                topicResponse.Partitions.push_back(std::move(topicPartitionResponse));
            }

            response.Responses.push_back(std::move(topicResponse));
        }

        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TKafkaProxyConfigPtr config,
    NNative::IConnectionPtr connection,
    IPollerPtr poller,
    IPollerPtr acceptor)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    for (int retryIndex = 0;; ++retryIndex) {
        try {
            auto listener = CreateListener(address, poller, acceptor, config->MaxBacklogSize);
            return New<TServer>(
                config,
                std::move(connection),
                std::move(poller),
                std::move(acceptor),
                std::move(listener));
        } catch (const std::exception& ex) {
            if (retryIndex + 1 == config->BindRetryCount) {
                throw;
            } else {
                YT_LOG_ERROR(ex, "Kafka proxy server bind failed");
                Sleep(config->BindRetryBackoff);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
