#include "server.h"

#include "config.h"
#include "connection.h"
#include "group_coordinator.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/kafka_proxy/records/kafka_message.record.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/client/kafka/protocol.h>
#include <yt/yt/client/kafka/error.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/auth_server/authentication_manager.h>
#include <yt/yt/library/auth_server/credentials.h>
#include <yt/yt/library/auth_server/token_authenticator.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/local_address.h>

#include <util/string/split.h>

namespace NYT::NKafkaProxy {

using namespace NApi;
using namespace NAuth;
using namespace NConcurrency;
using namespace NKafka;
using namespace NNet;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NQueueClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_KAFKA_HANDLER(method)                         \
    TRsp##method Do##method(                                 \
        [[maybe_unused]] TConnectionId connectionId,         \
        [[maybe_unused]] const TReq##method& request,        \
        const NLogging::TLogger& Logger)

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = KafkaProxyLogger;

////////////////////////////////////////////////////////////////////////////////

static const TString OAuthBearerSaslMechanism = "OAUTHBEARER";
static const TString PlainSaslMechanism = "PLAIN";

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        TProxyBootstrapConfigPtr config,
        NNative::IConnectionPtr connection,
        IAuthenticationManagerPtr authenticationManager,
        IPollerPtr poller,
        IPollerPtr acceptor,
        IListenerPtr listener)
        : Config_(std::move(config))
        , NativeConnection_(std::move(connection))
        , AuthenticationManager_(std::move(authenticationManager))
        , Poller_(std::move(poller))
        , Acceptor_(std::move(acceptor))
        , Listener_(std::move(listener))
        , DynamicConfig_(New<TProxyDynamicConfig>())
    {
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoApiVersions, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoMetadata, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoFindCoordinator, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoJoinGroup, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoSyncGroup, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoHeartbeat, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoOffsetFetch, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoOffsetCommit, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoFetch, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoSaslHandshake, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoSaslAuthenticate, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoProduce, Unretained(this)));
        RegisterTypedHandler(BIND_NO_PROPAGATE(&TServer::DoListOffsets, Unretained(this)));
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

    void OnDynamicConfigChanged(const TProxyDynamicConfigPtr& config) override
    {
        DynamicConfig_.Store(config);

        {
            auto guard = ReaderGuard(GroupCoordinatorMapLock_);
            for (const auto& [_, groupCoordinator] : GroupCoordinators_) {
                groupCoordinator->Reconfigure(config->GroupCoordinator);
            }
        }
    }

private:
    const TProxyBootstrapConfigPtr Config_;

    const NNative::IConnectionPtr NativeConnection_;
    const NNative::TClientCachePtr AuthenticatedClientCache_;
    const IAuthenticationManagerPtr AuthenticationManager_;

    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;
    const IListenerPtr Listener_;

    TAtomicIntrusivePtr<TProxyDynamicConfig> DynamicConfig_;

    std::atomic<bool> Started_ = false;

    struct TConnectionState final
    {
        explicit TConnectionState(IConnectionPtr connection)
            : Connection(std::move(connection))
        { }

        IConnectionPtr Connection;

        int SaslHandshakeVersion = 0;
        std::optional<TString> SaslMechanism;
        std::optional<ERequestType> ExpectedRequestType = {ERequestType::SaslHandshake};

        std::optional<TString> UserName;
    };
    using TConnectionStatePtr = TIntrusivePtr<TConnectionState>;

    THashMap<TConnectionId, TConnectionStatePtr> Connections_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConnectionMapLock_);

    using TGroupId = TString;
    THashMap<TGroupId, IGroupCoordinatorPtr> GroupCoordinators_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, GroupCoordinatorMapLock_);

    TEnumIndexedArray<ERequestType, THandler> Handlers_;

    TProxyDynamicConfigPtr GetDynamicConfig() const
    {
        return DynamicConfig_.Acquire();
    }

    std::string GetLocalHostName() const
    {
        auto dynamicConfig = GetDynamicConfig();
        return dynamicConfig->LocalHostName.value_or(NNet::GetLocalHostName());
    }

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
            connection->GetLocalAddress(),
            connection->GetRemoteAddress());

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
            YT_LOG_ERROR(ex, "Failed to process request "
                "(ConnectionId: %v)",
                connection->GetConnectionId());

            OnConnectionFailure(connection, TError(ex));
        }
    }

    TConnectionStatePtr GetConnectionState(TConnectionId connectionId)
    {
        auto guard = ReaderGuard(ConnectionMapLock_);
        auto connectionIt = Connections_.find(connectionId);
        if (connectionIt == Connections_.end()) {
            THROW_ERROR_EXCEPTION("Connection %v is not registered",
                connectionId);
        }
        return connectionIt->second;
    }

    void GuardedOnRequest(IConnectionPtr connection, TMessage request)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto response = ProcessRequest(connection, request);

        YT_UNUSED_FUTURE(connection->PostMessage(response)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    OnConnectionFailure(connection, error);
                }
            })));
    }

    TMessage ProcessRequest(
        const IConnectionPtr& connection,
        const TMessage& request)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (request.size() != 1) {
            THROW_ERROR_EXCEPTION("Incoming message has %v parts, expected 1",
                request.size());
        }

        auto connectionState = GetConnectionState(connection->GetConnectionId());
        auto expectedRequestType = connectionState->ExpectedRequestType;

        // For SaslHandshake v0 tokens are sent as opaque packets without wrapping the messages with Kafka protocol headers.
        // SaslHandshake v1 is not supported for now.
        if (expectedRequestType && *expectedRequestType == ERequestType::SaslAuthenticate && connectionState->SaslHandshakeVersion == 0) {
            auto response = DoSaslAuthenticate(
                connection->GetConnectionId(),
                TReqSaslAuthenticate{.AuthBytes = request.ToString()},
                Logger()
                    .WithTag("ConnectionId: %v", connection->GetConnectionId())
                    .WithTag("RequestType: %v", ERequestType::SaslAuthenticate));

            if (!response.ErrorMessage) {
                response.ErrorMessage = "Authentication failed";
            }

            auto builder = TSharedRefArrayBuilder(1);

            auto protocolWriter = NKafka::CreateKafkaProtocolWriter();
            response.Serialize(protocolWriter.get(), /*apiVersion*/ 0);

            YT_LOG_DEBUG("Response sent (RequestType: %v, ConnectionId: %v)",
                ERequestType::SaslAuthenticate,
                connection->GetConnectionId());

            builder.Add(protocolWriter->Finish());
            return builder.Finish();
        }

        auto reader = CreateKafkaProtocolReader(request[0]);

        TRequestHeader header;
        header.Deserialize(reader.get());

        YT_LOG_DEBUG("Request received (RequestType: %v, ApiVersion: %v, CorrelationId: %v, ClientId: %v, ConnectionId: %v)",
            header.RequestType,
            header.ApiVersion,
            header.CorrelationId,
            header.ClientId,
            connection->GetConnectionId());

        // ApiVersions request could be sent before SaslHandshake, so let's allow it always.
        if (expectedRequestType && header.RequestType != *expectedRequestType && header.RequestType != ERequestType::ApiVersions) {
            THROW_ERROR_EXCEPTION("Incoming request is %v, but %v was expected", header.RequestType, *expectedRequestType);
        }

        if (header.RequestType != ERequestType::SaslHandshake
            && header.RequestType != ERequestType::SaslAuthenticate
            && header.RequestType != ERequestType::ApiVersions) {
            // User should be authenticated, just ignore all other requests.
            if (!connectionState->UserName) {
                YT_LOG_DEBUG("User is unknown (RequestType: %v)", header.RequestType);
                return TSharedRefArrayBuilder(1).Finish();
            }
        }

        auto responseHeader = [&header] {
            auto protocolWriter = NKafka::CreateKafkaProtocolWriter();
            TResponseHeader result;
            result.CorrelationId = header.CorrelationId;
            result.Serialize(protocolWriter.get(), GetResponseHeaderVersion(header.RequestType, header.ApiVersion));
            return protocolWriter->Finish();
        }();

        if (auto handler = Handlers_[header.RequestType]; handler) {
            auto responseMessage = handler(connection->GetConnectionId(), reader.get(), header);

            YT_LOG_DEBUG("Response sent (RequestType: %v, CorrelationId: %v, ClientId: %v, ConnectionId: %v, HeaderSize: %v, MessageSize: %v)",
                header.RequestType,
                header.CorrelationId,
                header.ClientId,
                connection->GetConnectionId(),
                responseHeader.Size(),
                responseMessage.Size());

            TSharedRefArrayBuilder builder(2);
            builder.Add(responseHeader);
            builder.Add(responseMessage);

            return builder.Finish();
        } else {
            THROW_ERROR_EXCEPTION("Incoming message has invalid type %x, ignored",
                static_cast<int>(header.RequestType));
        }
    }

    void OnConnectionFailure(IConnectionPtr connection, TError error)
    {
        auto connectionId = connection->GetConnectionId();

        YT_LOG_INFO(
            error,
            "Kafka proxy server observed connection failure, terminating connection "
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
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(ConnectionMapLock_);
        if (Connections_.erase(connectionId)) {
            YT_LOG_DEBUG("Connection unregistered (ConnectionId: %v)",
                connectionId);
            return true;
        }

        return false;
    }

    DEFINE_KAFKA_HANDLER(ApiVersions)
    {
        YT_LOG_DEBUG("Start to handle ApiVersions request (ClientSoftwareName: %v)",
            request.ClientSoftwareName);

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
                .MaxVersion = 1,
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
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::JoinGroup),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::SyncGroup),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
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
                .MaxVersion = 1,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::SaslAuthenticate),
                .MinVersion = 0,
                .MaxVersion = 0,
            },
            TRspApiKey{
                .ApiKey = static_cast<int>(ERequestType::Produce),
                .MinVersion = 0,
                .MaxVersion = 8,
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

    DEFINE_KAFKA_HANDLER(SaslHandshake)
    {
        YT_LOG_DEBUG("Start to handle SaslHandshake request (Mechanism: %v)",
            request.Mechanism);

        TRspSaslHandshake response;
        response.Mechanisms = {PlainSaslMechanism, OAuthBearerSaslMechanism};
        if (std::find(response.Mechanisms.begin(), response.Mechanisms.end(), request.Mechanism) == response.Mechanisms.end()) {
            YT_LOG_DEBUG("Unsupported SASL mechanism (Requested: %v, Expected: %v)",
                request.Mechanism,
                response.Mechanisms);

            response.ErrorCode = NKafka::EErrorCode::UnsupportedSaslMechanism;
        }

        auto connectionState = GetConnectionState(connectionId);
        connectionState->ExpectedRequestType = ERequestType::SaslAuthenticate;
        connectionState->SaslMechanism = request.Mechanism;
        connectionState->SaslHandshakeVersion = request.ApiVersion;

        return response;
    }

    DEFINE_KAFKA_HANDLER(SaslAuthenticate)
    {
        YT_LOG_DEBUG("Start to handle SaslAuthenticate request");

        auto connectionState = GetConnectionState(connectionId);

        TRspSaslAuthenticate response;

        if (connectionState->UserName) {
            YT_LOG_DEBUG("Client was authenticated before");
            return response;
        }

        auto fillError = [&response] (const TString& message) {
            response.ErrorCode = NKafka::EErrorCode::SaslAuthenticationFailed;
            response.ErrorMessage = message;
        };

        if (!connectionState->SaslMechanism) {
            fillError("Unknown SASL mechanism");
            return response;
        }

        auto splitByString = [] (const TString& input, const char* delimiter) {
            TVector<TString> parts;
            StringSplitter(input).SplitByString(delimiter).Collect(&parts);
            return parts;
        };

        auto splitByChar = [] (const TString& input, char delimiter) {
            TVector<TString> parts;
            StringSplitter(input).Split(delimiter).Collect(&parts);
            return parts;
        };

        TString token;
        std::optional<TString> expectedUserName;

        if (*connectionState->SaslMechanism == OAuthBearerSaslMechanism) {
            YT_LOG_DEBUG("Authenticating using OAUTHBEARER SASL mechanism");
            TString authBytes = request.AuthBytes;
            auto parts = splitByString(authBytes, "\x01");
            if (parts.size() < 2) {
                fillError(Format("Unexpected \"auth_bytes\" format, got %v \\x01-separated parts", parts.size()));
                return response;
            }
            parts = splitByString(parts[1], " ");
            if (parts.size() < 2) {
                fillError(Format("Unexpected \"auth_bytes\" format, got %v space-separated parts", parts.size()));
                return response;
            }
            token = parts[1];
        } else if (*connectionState->SaslMechanism == PlainSaslMechanism) {
            YT_LOG_DEBUG("Authenticating using PLAIN SASL mechanism");
            TString authBytes = request.AuthBytes;
            auto parts = splitByChar(authBytes, '\0');
            if (parts.size() < 3) {
                fillError(Format("Unexpected \"auth_bytes\" format, got %v \\0-separated parts", parts.size()));
                return response;
            }

            expectedUserName = parts[1];
            token = parts[2];
        } else {
            fillError(Format("Unknown SASL mechanism %Qv", *connectionState->SaslMechanism));
            return response;
        }

        auto authenticator = AuthenticationManager_->GetTokenAuthenticator();
        auto authResultOrError = WaitFor(authenticator->Authenticate(TTokenCredentials{.Token = std::move(token)}));
        if (!authResultOrError.IsOK()) {
            auto error = TError("Failed to authenticate user")
                << TErrorAttribute("connection_id", connectionId)
                << authResultOrError;
            YT_LOG_DEBUG(error);
            fillError(ToString(error));
            return response;
        }

        auto login = authResultOrError.Value().Login;
        if (expectedUserName && *expectedUserName != login) {
            YT_LOG_DEBUG("Failed to authenticate user (AuthenticatedUserName: %v, ExpectedUserName: %v)",
                login,
                *expectedUserName);
            fillError(Format("User %Qv was expected", *expectedUserName));
            return response;
        }

        connectionState->UserName = authResultOrError.Value().Login;
        connectionState->ExpectedRequestType = std::nullopt;

        YT_LOG_DEBUG("Authentication successful (UserName: %v)",
            *connectionState->UserName);

        return response;
    }

    DEFINE_KAFKA_HANDLER(Metadata)
    {
        YT_LOG_DEBUG("Start to handle Metadata request (TopicsSize: %v)",
            request.Topics.size());

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        TRspMetadata response;
        response.Brokers = std::vector<TRspMetadataBroker>{
            TRspMetadataBroker{
                .NodeId = 0,
                .Host = GetLocalHostName(),
                .Port = Config_->Port,
                .Rack = "1",
            },
        };

        response.Topics.reserve(request.Topics.size());
        for (const auto& topic : request.Topics) {
            auto path = TRichYPath::Parse(topic.Topic);
            auto tableInfo = WaitFor(NativeConnection_->GetTableMountCache()->GetTableInfo(path.GetPath()))
                .ValueOrThrow();

            NSecurityClient::TPermissionKey permissionKey{
                .Object = FromObjectId(tableInfo->TableId),
                .User = *userName,
                .Permission = EPermission::Read,
            };
            const auto& permissionCache = NativeConnection_->GetPermissionCache();
            auto hasPermission =  WaitFor(permissionCache->Get(permissionKey)).IsOK();

            TRspMetadataTopic topicResponse{
                .Name = topic.Topic,
                .TopicId = topic.TopicId,
            };

            if (!hasPermission) {
                topicResponse.ErrorCode = NKafka::EErrorCode::TopicAuthorizationFailed;
            } else {
                topicResponse.Partitions.reserve(tableInfo->Tablets.size());
                for (int tabletIndex = 0; tabletIndex < std::ssize(tableInfo->Tablets); ++tabletIndex) {
                    topicResponse.Partitions.push_back(TRspMetadataTopicPartition{
                        .PartitionIndex = tabletIndex,
                        .LeaderId = 0,
                        .ReplicaNodes = {0},
                    });
                }
            }

            response.Topics.push_back(std::move(topicResponse));
        }

        return response;
    }

    DEFINE_KAFKA_HANDLER(FindCoordinator)
    {
        YT_LOG_DEBUG("Start to handle FindCoordinator request (Key: %v)",
            request.Key);

        TRspFindCoordinator response;
        response.NodeId = 0;
        response.Host = GetLocalHostName();
        response.Port = Config_->Port;

        return response;
    }

    DEFINE_KAFKA_HANDLER(JoinGroup)
    {
        YT_LOG_DEBUG("Start to handle JoinGroup request (GroupId: %v, MemberId: %v, ProtocolType: %v)",
            request.GroupId,
            request.MemberId,
            request.ProtocolType);

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));

        // TODO(nadya73): check permissions and return GROUP_AUTHORIZATION_FAILED.

        auto dynamicConfig = GetDynamicConfig();

        IGroupCoordinatorPtr groupCoordinator;
        {
            auto guard = WriterGuard(GroupCoordinatorMapLock_);
            auto groupCoordinatorIt = GroupCoordinators_.find(request.GroupId);
            if (groupCoordinatorIt != GroupCoordinators_.end()) {
                groupCoordinator = groupCoordinatorIt->second;
            } else {
                groupCoordinator = CreateGroupCoordinator(request.GroupId, dynamicConfig->GroupCoordinator);
                GroupCoordinators_[request.GroupId] = groupCoordinator;
            }
        }

        return groupCoordinator->JoinGroup(request, Logger.WithTag("GroupId: %v", request.GroupId));
    }

    DEFINE_KAFKA_HANDLER(SyncGroup)
    {
        YT_LOG_DEBUG("Start to handle SyncGroup request (GroupId: %v, MemberId: %v)",
            request.GroupId,
            request.MemberId);

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));

        // TODO(nadya73): check permissions and return GROUP_AUTHORIZATION_FAILED.

        IGroupCoordinatorPtr groupCoordinator;
        {
            auto guard = ReaderGuard(GroupCoordinatorMapLock_);
            auto groupCoordinatorIt = GroupCoordinators_.find(request.GroupId);
            if (groupCoordinatorIt != GroupCoordinators_.end()) {
                groupCoordinator = groupCoordinatorIt->second;
            } else {
                YT_LOG_DEBUG("Unknown group id (GroupId: %v)", request.GroupId);
                return TRspSyncGroup{ .ErrorCode = NKafka::EErrorCode::NotCoordinator };
            }
        }

        return groupCoordinator->SyncGroup(
            request,
            Logger
                .WithTag("GroupId: %v", request.GroupId)
                .WithTag("MemberId: %v", request.MemberId));
    }

    DEFINE_KAFKA_HANDLER(Heartbeat)
    {
        YT_LOG_DEBUG("Start to handle Heartbeat request (GroupId: %v, MemberId: %v)",
            request.GroupId,
            request.MemberId);

        IGroupCoordinatorPtr groupCoordinator;
        {
            auto guard = ReaderGuard(GroupCoordinatorMapLock_);
            auto groupCoordinatorIt = GroupCoordinators_.find(request.GroupId);
            if (groupCoordinatorIt != GroupCoordinators_.end()) {
                groupCoordinator = groupCoordinatorIt->second;
            } else {
                YT_LOG_DEBUG("Unknown group id (GroupId: %v)", request.GroupId);
                return TRspHeartbeat{ .ErrorCode = NKafka::EErrorCode::NotCoordinator };
            }
        }

        return groupCoordinator->Heartbeat(request, Logger.WithTag("GroupId: %v", request.GroupId));
    }

    DEFINE_KAFKA_HANDLER(OffsetCommit)
    {
        YT_LOG_DEBUG("Start to handle OffsetCommit request (GroupId: %v)", request.GroupId);

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        TRspOffsetCommit response;
        response.Topics.reserve(request.Topics.size());

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));
        YT_VERIFY(NativeConnection_->GetClusterName());

        auto fillResponse = [&](NKafka::EErrorCode errorCode = NKafka::EErrorCode::UnknownServerError) {
            for (const auto& topic : request.Topics) {
                auto& topicResponse = response.Topics.emplace_back();
                topicResponse.Partitions.reserve(topic.Partitions.size());
                for (const auto& partition : topic.Partitions) {
                    auto& partitionResponse = topicResponse.Partitions.emplace_back();
                    partitionResponse.PartitionIndex = partition.PartitionIndex;
                    partitionResponse.ErrorCode = errorCode;
                }
            }
        };

        auto transactionOrError = WaitFor(client->StartTransaction(ETransactionType::Tablet));
        if (!transactionOrError.IsOK()) {
            YT_LOG_DEBUG(transactionOrError,
                "Failed to start transaction (GroupId: %v)",
                request.GroupId);
            fillResponse();
            return response;
        }

        auto& transaction = transactionOrError.Value();

        auto consumerPath = TRichYPath::Parse(request.GroupId);

        for (const auto& topic : request.Topics) {
            auto queuePath = TRichYPath::Parse(topic.Name);
            for (const auto& partition : topic.Partitions) {
                auto advanceResultOrError = WaitFor(transaction->AdvanceQueueConsumer(
                    consumerPath, queuePath, partition.PartitionIndex, /*oldOffset*/ std::nullopt, partition.CommittedOffset, TAdvanceQueueConsumerOptions{}));
                if (!advanceResultOrError.IsOK()) {
                    YT_LOG_DEBUG(advanceResultOrError,
                        "Failed to advance consumer (ConsumerPath: %v, QueuePath: %v, PartitionIndex: %v, Offset: %v)",
                        consumerPath,
                        queuePath,
                        partition.PartitionIndex,
                        partition.CommittedOffset);
                    if (advanceResultOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
                        fillResponse(NKafka::EErrorCode::TopicAuthorizationFailed);
                    } else {
                        fillResponse();
                    }
                    fillResponse();
                    return response;
                }
            }
        }

        auto commitResultOrError = WaitFor(transaction->Commit());
        if (!commitResultOrError.IsOK()) {
            YT_LOG_DEBUG(commitResultOrError,
                "Failed to commit transaction (GroupId: %v)",
                request.GroupId);
            if (commitResultOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
                fillResponse(NKafka::EErrorCode::TopicAuthorizationFailed);
            } else {
                fillResponse();
            }
            return response;
        }

        fillResponse(NKafka::EErrorCode::None);

        return response;
    }

    DEFINE_KAFKA_HANDLER(OffsetFetch)
    {
        YT_LOG_DEBUG("Start to handle OffsetFetch request (GroupId: %v)",
            request.GroupId);

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        TRspOffsetFetch response;
        response.Topics.reserve(request.Topics.size());

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));
        YT_VERIFY(NativeConnection_->GetClusterName());

        auto path = TRichYPath::Parse(request.GroupId);
        auto consumerClient = CreateConsumerClient(client, path.GetPath());

        for (const auto& topic : request.Topics) {
            TRspOffsetFetchTopic topicResponse{
                .Name = topic.Name,
            };
            topicResponse.Partitions.reserve(topic.PartitionIndexes.size());

            // TODO(nadya73): add CollectPartitions in IConsumerClient too.
            auto subConsumerClient = consumerClient->GetSubConsumerClient(client, TCrossClusterReference::FromString(topic.Name));
            auto partitionsOrError = WaitFor(subConsumerClient->CollectPartitions(topic.PartitionIndexes));

            if (!partitionsOrError.IsOK()) {
                YT_LOG_DEBUG(partitionsOrError, "Failed to get partitions");
                for (auto partitionIndex : topic.PartitionIndexes) {
                    topicResponse.Partitions.push_back(TRspOffsetFetchTopicPartition{
                        // TODO(nadya73): add type check.
                        .PartitionIndex = static_cast<int32_t>(partitionIndex),
                        .ErrorCode = NKafka::EErrorCode::GroupAuthorizationFailed,
                    });
                }
            } else {
                for (const auto& partition : partitionsOrError.Value()) {
                    topicResponse.Partitions.push_back(TRspOffsetFetchTopicPartition{
                        // TODO(nadya73): add type check.
                        .PartitionIndex = static_cast<int32_t>(partition.PartitionIndex),
                        .CommittedOffset = partition.NextRowIndex,
                    });
                }
            }

            response.Topics.push_back(std::move(topicResponse));
        }

        return response;
    }

    DEFINE_KAFKA_HANDLER(Fetch)
    {
        YT_LOG_DEBUG("Start to handle Fetch request (TopicCount: %v)",
            request.Topics.size());

        // TODO(nadya73): log requested offsets.

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        TRspFetch response;
        response.Responses.reserve(request.Topics.size());

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));

        for (const auto& topic : request.Topics) {
            auto& topicResponse = response.Responses.emplace_back();
            topicResponse.Topic = topic.Topic;
            topicResponse.Partitions.reserve(topic.Partitions.size());

            for (const auto& partition : topic.Partitions) {
                auto& topicPartitionResponse = topicResponse.Partitions.emplace_back();
                topicPartitionResponse.PartitionIndex = partition.Partition;
                topicPartitionResponse.HighWatermark = 0;  // TODO(nadya73): fill it with normal data.

                auto rowsetOrError = WaitFor(client->PullQueue(
                    topic.Topic,
                    partition.FetchOffset,
                    partition.Partition,
                    TQueueRowBatchReadOptions{.MaxDataWeight = partition.PartitionMaxBytes}));

                if (!rowsetOrError.IsOK()) {
                    topicPartitionResponse.ErrorCode = NKafka::EErrorCode::TopicAuthorizationFailed;
                } else {
                    auto rowset = rowsetOrError.Value();

                    YT_LOG_DEBUG("Rows were fetched (Topic: %v, PartitionIndex: %v, Count: %v)",
                        topic.Topic,
                        partition.Partition,
                        rowset->GetRows().size());

                    auto messages = ConvertQueueRowsToMessages(rowset);

                    if (messages.size() > 0) {
                        topicPartitionResponse.Records = std::vector<TRecord>{};
                        topicPartitionResponse.Records->reserve(messages.size());

                        auto offset = rowsetOrError.Value()->GetStartOffset();

                        for (auto& message : messages) {
                            topicPartitionResponse.Records->push_back(TRecord{
                                .FirstOffset = offset,
                                .MagicByte = 1,
                                .Messages = {std::move(message)},
                            });
                            ++offset;
                        }
                    }
                }
            }
        }

        return response;
    }

    DEFINE_KAFKA_HANDLER(Produce)
    {
        YT_LOG_DEBUG("Start to handle Produce request (TopicCount: %v)",
            request.TopicData.size());

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        TRspProduce response;
        response.Responses.reserve(request.TopicData.size());

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));

        for (const auto& topic : request.TopicData) {
            auto& topicResponse = response.Responses.emplace_back();
            topicResponse.PartitionResponses.reserve(topic.PartitionData.size());

            auto path = TRichYPath::Parse(topic.Name);

            auto fillError = [&](NKafka::EErrorCode errorCode = NKafka::EErrorCode::UnknownServerError) {
                for (const auto& partition : topic.PartitionData) {
                    auto& partitionResponse = topicResponse.PartitionResponses.emplace_back();
                    partitionResponse.Index = partition.Index;
                    partitionResponse.ErrorCode = errorCode;
                }
            };

            auto transactionOrError = WaitFor(client->StartTransaction(ETransactionType::Tablet));
            if (!transactionOrError.IsOK()) {
                YT_LOG_DEBUG(transactionOrError,
                    "Failed to produce rows (Topic: %v)",
                    topic.Name);
                fillError();
                continue;
            }

            auto& transaction = transactionOrError.Value();

            auto nameTable = NKafka::NRecords::TKafkaMessageDescriptor::Get()->GetNameTable();

            for (const auto& partition : topic.PartitionData) {
                std::vector<NKafka::NRecords::TKafkaMessagePartial> messages;
                messages.reserve(partition.Records.size());
                for (const auto& record : partition.Records) {
                    for (const auto& message : record.Messages) {
                        messages.push_back(NKafka::NRecords::TKafkaMessagePartial{
                            .TabletIndex = partition.Index,
                            .MessageKey = message.Key,
                            .MessageValue = message.Value,
                        });
                    }
                }

                auto rows = FromRecords(TRange(messages));
                transaction->WriteRows(path.GetPath(), nameTable, rows);
            }

            auto commitResultOrError = WaitFor(transaction->Commit());

            if (!commitResultOrError.IsOK()) {
                YT_LOG_DEBUG(commitResultOrError,
                    "Failed to produce rows (Topic: %v)",
                    topic.Name);

                if (commitResultOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
                    fillError(NKafka::EErrorCode::TopicAuthorizationFailed);
                } else {
                    fillError();
                }
                continue;
            }

            for (const auto& partition : topic.PartitionData) {
                topicResponse.PartitionResponses.push_back(TRspProduceResponsePartitionResponse{
                    .Index = partition.Index,
                });
            }
        }

        return response;
    }

    DEFINE_KAFKA_HANDLER(ListOffsets)
    {
        YT_LOG_DEBUG("Start to handle ListOffsets request");

        auto connectionState = GetConnectionState(connectionId);

        auto userName = GetConnectionState(connectionId)->UserName;
        if (!userName) {
            THROW_ERROR_EXCEPTION("Unknown user name, something went wrong");
        }

        auto client = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(*userName));

        TRspListOffsets response;
        response.Topics.reserve(request.Topics.size());

        for (const auto& topic : request.Topics) {
            auto& topicResponse = response.Topics.emplace_back();
            topicResponse.Partitions.reserve(topic.Partitions.size());

            topicResponse.Name = topic.Name;

            std::vector<int> tabletIndexes;
            tabletIndexes.reserve(topic.Partitions.size());
            std::transform(topic.Partitions.begin(), topic.Partitions.end(), std::back_inserter(tabletIndexes),
                [] (const auto& partition) {
                    return static_cast<int>(partition.PartitionIndex);
                });

            auto tabletInfos = WaitFor(client->GetTabletInfos(topic.Name, tabletIndexes))
                .ValueOrThrow();

            for (int partitionIndex = 0; partitionIndex < std::ssize(topic.Partitions); ++partitionIndex) {
                const auto& partition = topic.Partitions[partitionIndex];
                auto& partitionResponse = topicResponse.Partitions.emplace_back();

                partitionResponse.PartitionIndex = partition.PartitionIndex;

                if (partition.Timestamp == -2) {
                    partitionResponse.Offset = 0;
                } else if (partition.Timestamp == -1) {
                    partitionResponse.Offset = tabletInfos[partitionIndex].TotalRowCount;
                } else {
                    partitionResponse.ErrorCode = NKafka::EErrorCode::InvalidTimestamp;
                }
            }
        }

        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TProxyBootstrapConfigPtr config,
    NNative::IConnectionPtr connection,
    IAuthenticationManagerPtr authenticationManager,
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
                std::move(authenticationManager),
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
