#include "yt_connector.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/client/objects/helpers.h>

#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/federated/connection.h>

#include <yt/yt/client/api/client_cache.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NOrm::NServer::NMaster {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NCypressClient;
using namespace NYT::NTransactionClient;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NYson;

using NClient::NObjects::IsValidInstanceTag;
using NClient::NObjects::ForEachValidInstanceTag;
using NClient::NObjects::ValidInstanceTagRange;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString YTTvmAlias = "yt";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInstanceTagAllocator)

class TInstanceTagAllocator
    : public TRefCounted
{
public:
    TInstanceTagAllocator(
        NYPath::TYPath root,
        TDuration instanceTagExpirationTimeout)
        : Root_(std::move(root))
        , InstanceTagExpirationTimeout_(instanceTagExpirationTimeout)
    { }

    TMasterInstanceTag Allocate(
        const IClientPtr& client,
        const ITransactionPtr& transaction,
        const TString& fqdn,
        TMasterInstanceTag tagFromConfig)
    {
        YT_LOG_INFO("Allocating instance tag (TransactionId: %v, Fqdn: %v, TagFromConfig: %v)",
            transaction->GetId(),
            fqdn,
            tagFromConfig);

        TMasterInstanceTag tag{UndefinedMasterInstanceTag};
        if (tagFromConfig != UndefinedMasterInstanceTag) {
            YT_LOG_INFO("Picked instance tag from config (Tag: %v)",
                tagFromConfig);
            tag = tagFromConfig;
        } else {
            tag = PickTag(client, fqdn);
        }

        LockTag(client, transaction, fqdn, tag);

        return tag;
    }

private:
    struct TTagStatus
    {
        TString Fqdn;
        TInstant AllocationTime = TInstant::Zero();
        bool Locked = false;
    };

    const NYPath::TYPath Root_;
    const TDuration InstanceTagExpirationTimeout_;

    THashMap<TMasterInstanceTag, TTagStatus> ListTags(const IClientPtr& client)
    {
        YT_LOG_INFO("Listing instance tags");

        TGetNodeOptions options;
        options.Attributes = std::vector<TString>{
            "lock_count",
            "value"};
        auto tagNodesYson = WaitFor(client->GetNode(Root_, std::move(options)))
            .ValueOrThrow();

        THashMap<TMasterInstanceTag, TTagStatus> result;

        auto tagNodes = ConvertTo<IMapNodePtr>(tagNodesYson);
        for (const auto& [tagString, tagNode] : tagNodes->GetChildren()) {
            auto tag = TMasterInstanceTag{FromString<ui8>(tagString)};
            if (!IsValidInstanceTag(tag)) {
                THROW_ERROR_EXCEPTION("Expected instance tag in range %v, but got %v",
                    ValidInstanceTagRange(),
                    tag);
            }

            TTagStatus status;

            auto tagMapNode = ConvertTo<IMapNodePtr>(tagNode);
            status.Locked = tagMapNode->Attributes().Get<int>("lock_count") > 0;

            if (auto tagStatusNode = tagMapNode->FindChild("status")) {
                auto tagStatusValueNode = tagStatusNode->Attributes().Get<IMapNodePtr>("value");
                if (auto statusFqdnNode = tagStatusValueNode->FindChild("fqdn")) {
                    status.Fqdn = ConvertTo<TString>(statusFqdnNode);
                }
                if (auto statusAllocationTimeNode = tagStatusValueNode->FindChild("allocation_time")) {
                    status.AllocationTime = ConvertTo<TInstant>(statusAllocationTimeNode);
                }
            }

            YT_LOG_DEBUG("Found instance tag (Tag: %v, Fqdn: %v, AllocationTime: %v, Locked: %v)",
                tag,
                status.Fqdn,
                status.AllocationTime,
                status.Locked);

            EmplaceOrCrash(result, tag, std::move(status));
        }

        return result;
    }

    TMasterInstanceTag PickTag(const IClientPtr& client, const TString& fqdn)
    {
        YT_LOG_INFO("Picking instance tag (Fqdn: %v)",
            fqdn);

        auto tags = ListTags(client);

        for (const auto& [tag, status] : tags) {
            if (status.Fqdn == fqdn) {
                YT_LOG_INFO("Picked previously used instance tag by the given fqdn (Tag: %v, Fqdn: %v)",
                    tag,
                    fqdn);
                return tag;
            }
        }

        std::vector<TMasterInstanceTag> nonexistentTags;
        std::vector<TMasterInstanceTag> unlockedTags;
        ForEachValidInstanceTag([&] (TMasterInstanceTag tag) {
            auto it = tags.find(tag);
            if (it == tags.end()) {
                nonexistentTags.push_back(tag);
            } else if (!it->second.Locked) {
                unlockedTags.push_back(tag);
            }
        });

        if (nonexistentTags.size() > 0) {
            auto tag = nonexistentTags[RandomNumber<size_t>(nonexistentTags.size())];
            YT_LOG_INFO("Picked nonexistent instance tag (Tag: %v)",
                tag);
            return tag;
        }

        if (unlockedTags.size() > 0) {
            auto tag = unlockedTags[RandomNumber<size_t>(unlockedTags.size())];
            YT_LOG_INFO("Picked existent but unlocked instance tag (Tag: %v)",
                tag);
            return tag;
        }

        THROW_ERROR_EXCEPTION("Failed to pick instance tag within range %v",
            ValidInstanceTagRange());
    }

    void LockTag(const IClientPtr& client, const ITransactionPtr& transaction, const TString& fqdn, TMasterInstanceTag tag)
    {
        auto Logger = NMaster::Logger()
            .WithTag("TransactionId: %v", transaction->GetId())
            .WithTag("Fqdn: %v", fqdn)
            .WithTag("Tag: %v", tag);

        YT_LOG_INFO("Locking instance tag");

        YT_VERIFY(IsValidInstanceTag(tag));
        auto instanceTagNodePath = Format("%v/%v", Root_, tag);
        auto instanceTagStatusNodePath = Format("%v/%v", instanceTagNodePath, "status");

        YT_LOG_INFO("Setting instance tag node expiration timeout");
        {
            TCreateNodeOptions options;
            options.Recursive = true;
            options.Force = true;
            options.Attributes = CreateEphemeralAttributes();
            options.Attributes->Set("expiration_timeout", InstanceTagExpirationTimeout_);
            WaitFor(client->CreateNode(
                instanceTagNodePath,
                EObjectType::MapNode,
                std::move(options)))
                .ThrowOnError();
        }

        YT_LOG_INFO("Ensuring instance tag status node exists");
        {
            TCreateNodeOptions options;
            options.Force = true;
            options.Attributes = CreateEphemeralAttributes();
            options.Attributes->Set("value", GetEphemeralNodeFactory()->CreateMap());
            WaitFor(client->CreateNode(
                instanceTagStatusNodePath,
                EObjectType::Document,
                std::move(options)))
                .ThrowOnError();
        }

        YT_LOG_INFO("Locking instance tag node");
        WaitFor(transaction->LockNode(
            instanceTagNodePath,
            ELockMode::Exclusive))
            .ThrowOnError();

        YT_LOG_INFO("Setting instance tag status node");
        {
            TSetNodeOptions options;
            options.PrerequisiteTransactionIds.push_back(transaction->GetId());
            WaitFor(client->SetNode(
                instanceTagStatusNodePath,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("allocation_time").Value(TInstant::Now())
                        .Item("fqdn").Value(fqdn)
                    .EndMap(),
                std::move(options)))
                .ThrowOnError();
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TInstanceTagAllocator)

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TYTConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        IBootstrap* bootstrap,
        TYTConnectorConfigPtr config,
        int expectedDBVersion,
        std::vector<TString> dynamicAttributeKeys,
        TMasterDiscoveryInfoValidator masterDiscoveryInfoValidator)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , ExpectedDBVersion_(expectedDBVersion)
        , DynamicAttributeKeys_(std::move(dynamicAttributeKeys))
        , MasterDiscoveryInfoValidator_(std::move(masterDiscoveryInfoValidator))
        , ServiceTicketAuth_(TryCreateServiceTicketAuth())
        , MasterDiscoveryExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnMasterDiscovery, MakeWeak(this)),
            Config_->MasterDiscoveryPeriod))
        , BanCheckExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnBanCheck, MakeWeak(this)),
            Config_->BanCheckPeriod))
        , DBPath_(GetRootPath() + "/db")
        , MasterPath_(GetRootPath() + "/master")
        , ConsumersPath_(GetConsumersPath())
        , NativeConnection_(TryCreateNativeConnection())
        , NativeClientCache_(TryCreateNativeClientCache())
        , Connection_(NativeConnection_ ? NativeConnection_ : CreateRpcProxyConnection())
        , FederatedConnection_(Config_->FederatedConnection ? CreateFederatedConnection() : nullptr)
        , ClientCache_(NativeClientCache_ ? NativeClientCache_ : CreateClientCache())
        , ControlClient_(CreateControlClient())
        , InstanceTagAllocator_(New<TInstanceTagAllocator>(
            GetInstanceTagsPath(),
            Config_->NodeExpirationTimeout))
    {
        YT_VERIFY(GetRootPath());
    }

    void Initialize()
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetControlInvoker(),
            ControlThread);

        YT_LOG_INFO("YT connector has been initialized (DBPath: %v)",
            DBPath_);
    }

    void Start()
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetControlInvoker(),
            ControlThread);

        MasterDiscoveryExecutor_->Start();
        BanCheckExecutor_->Start();

        ScheduleConnect(true);
    }

    IClientPtr GetControlClient()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(ControlClient_);
        return ControlClient_;
    }

    std::string FormatUserTag(std::string userTag) const
    {
        if (userTag.empty()) {
            auto identity = NAccessControl::TryGetAuthenticatedUserIdentity().value_or(
                NRpc::GetCurrentAuthenticationIdentity());
            if (identity.User != identity.UserTag) {
                userTag = identity.User + ":" + identity.UserTag;
            } else {
                userTag = identity.User;
            }
        }
        return Format("%v:%v", Config_->User, userTag);
    }

    IClientPtr GetClient(TString identityUserTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(ClientCache_);
        return GetClientImpl<IClientPtr>(ClientCache_, std::move(identityUserTag));
    }

    NNative::IClientPtr GetNativeClient(TString identityUserTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(NativeClientCache_);
        return GetClientImpl<NNative::IClientPtr>(NativeClientCache_, std::move(identityUserTag));
    }

    NNative::IConnectionPtr GetNativeConnection() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(NativeConnection_);
        return NativeConnection_;
    }

    const TYPath& GetRootPath()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_->RootPath;
    }

    TYPath GetConsumersPath()
    {
        return YPathJoin(Config_->RootPath, Config_->ConsumerDir);
    }

    const TYPath& GetDBPath()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return DBPath_;
    }

    const TYPath& GetMasterPath()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return MasterPath_;
    }

    TYPath GetConsumerPath(std::string_view consumer)
    {
        return YPathJoin(ConsumersPath_, consumer);
    }

    TYPath GetTablePath(const NObjects::TDBTable* table)
    {
        return YPathJoin(DBPath_, table->GetName());
    }

    TClusterTag GetClusterTag()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_->ClusterTag;
    }

    const std::string& GetClusterName()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_->ClusterName;
    }

    TMasterInstanceTag GetInstanceTag()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return InstanceTag_.load();
    }

    const TYTConnectorConfigPtr& GetConfig()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Config_;
    }

    bool IsConnected() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return IsConnected_.load(std::memory_order::relaxed);
    }

    TError CheckConnected() const
    {
        if (IsConnected()) {
            return TError();
        } else {
            return TError(NRpc::EErrorCode::Unavailable,
                "Master is not connected to YT");
        }
    }

    bool IsLeading() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return IsLeading_.load(std::memory_order::relaxed);
    }

    const ITransactionPtr& GetInstanceTransaction()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        return InstanceTransaction_;
    }

    int GetExpectedDBVersion() const
    {
        return ExpectedDBVersion_;
    }

    std::vector<TMasterDiscoveryInfo> GetMasters()
    {
        return MasterDiscoveryInfos_.Load();
    }

    DEFINE_SIGNAL(void(), Connected);
    DEFINE_SIGNAL(void(), Disconnected);
    DEFINE_SIGNAL(void(), ValidateConnection);

    DEFINE_SIGNAL(void(), StartedLeading);
    DEFINE_SIGNAL(void(), StoppedLeading);

private:
    IBootstrap* const Bootstrap_;
    const TYTConnectorConfigPtr Config_;
    const int ExpectedDBVersion_;
    const std::vector<TString> DynamicAttributeKeys_;
    const TMasterDiscoveryInfoValidator MasterDiscoveryInfoValidator_;
    const NAuth::IServiceTicketAuthPtr ServiceTicketAuth_;

    const TPeriodicExecutorPtr MasterDiscoveryExecutor_;
    const TPeriodicExecutorPtr BanCheckExecutor_;

    const TYPath DBPath_;
    const TYPath MasterPath_;
    const TYPath ConsumersPath_;

    const NNative::IConnectionPtr NativeConnection_;
    const NNative::TClientCachePtr NativeClientCache_;

    const IConnectionPtr Connection_;
    const IConnectionPtr FederatedConnection_;
    const TClientCachePtr ClientCache_;
    const IClientPtr ControlClient_;

    const TInstanceTagAllocatorPtr InstanceTagAllocator_;

    std::atomic<TMasterInstanceTag> InstanceTag_ = {UndefinedMasterInstanceTag};

    ITransactionPtr InstanceTransaction_;
    ITransactionPtr LeaderTransaction_;

    bool IsConnectScheduled_ = false;
    std::atomic<bool> IsConnected_ = {false};

    bool IsTakeLeaderLockScheduled_ = false;
    std::atomic<bool> IsLeading_ = {false};

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    TAtomicObject<std::vector<TMasterDiscoveryInfo>> MasterDiscoveryInfos_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

////////////////////////////////////////////////////////////////////////////////

    NAuth::IServiceTicketAuthPtr TryCreateServiceTicketAuth() const
    {
        if (!Config_->UseServiceTicketAuth) {
            return nullptr;
        }
        auto tvmServiceConfig = Bootstrap_->GetInitialConfig()->TvmService;
        if (tvmServiceConfig &&
            tvmServiceConfig->ClientEnableServiceTicketFetching &&
            tvmServiceConfig->ClientDstMap.contains(YTTvmAlias))
        {
            auto tvmService = Bootstrap_->GetTvmService();
            YT_VERIFY(tvmService);
            return NAuth::CreateServiceTicketAuth(tvmService, YTTvmAlias);
        }
        YT_LOG_WARNING("\"use_service_ticket_auth\" provided, but tvm service is not configured");
        return nullptr;
    }

    NNative::IConnectionPtr TryCreateNativeConnection() const
    {
        if (Config_->RpcProxyConnection) {
            return nullptr;
        }

        NNative::TConnectionCompoundConfigPtr connectionConfig;
        if (!Config_->ConnectionClusterUrl.empty()) {
            // Create RPC client to validate #user and #token and to get native connection options.
            auto rpcConnectionConfig = New<NYT::NApi::NRpcProxy::TConnectionConfig>();
            rpcConnectionConfig->ClusterUrl = Config_->ConnectionClusterUrl;
            auto client = NYT::NApi::NRpcProxy::CreateConnection(rpcConnectionConfig)->CreateClient({
                .User = Config_->User,
                .Token = Config_->GetToken(),
            });

            connectionConfig = New<NNative::TConnectionCompoundConfig>(
                New<NNative::TConnectionStaticConfig>(),
                New<NNative::TConnectionDynamicConfig>());
            auto clusterConnection = WaitFor(client->GetNode("//sys/@cluster_connection"))
                .ValueOrThrow();
            ReconfigureYsonStruct(
                connectionConfig->Static,
                clusterConnection);
            ReconfigureYsonStruct(
                connectionConfig->Dynamic,
                clusterConnection);

            if (Config_->Connection) {
                // Sanity check that #ConnectionClusterUrl matches #Config_->Connection.
                if (Config_->Connection->Static->PrimaryMaster &&
                    Config_->Connection->Static->PrimaryMaster->CellId != connectionConfig->Static->PrimaryMaster->CellId)
                {
                    THROW_ERROR_EXCEPTION("\"connection_cluster_url\" does not match native connection options");
                }
                // Merge with user specified connection options.
                UpdateYsonStruct(
                    connectionConfig->Static,
                    ConvertTo<INodePtr>(Config_->Connection));
                UpdateYsonStruct(
                    connectionConfig->Dynamic,
                    ConvertTo<INodePtr>(Config_->Connection));
            }
        } else {
            connectionConfig = Config_->Connection;
        }

        NNative::TConnectionOptions options;
        options.ConnectionInvoker = Bootstrap_->GetWorkerPoolInvoker();
        options.RetryRequestQueueSizeLimitExceeded = true;
        return NNative::CreateConnection(connectionConfig, std::move(options));
    }

    NNative::TClientCachePtr TryCreateNativeClientCache() const
    {
        if (NativeConnection_) {
            return New<NNative::TClientCache>(Config_->ClientCache, NativeConnection_);
        } else {
            return nullptr;
        }
    }

    IConnectionPtr CreateRpcProxyConnection() const
    {
        YT_VERIFY(Config_->RpcProxyConnection);
        NYT::NApi::NRpcProxy::TConnectionOptions options;
        options.ConnectionInvoker = Bootstrap_->GetWorkerPoolInvoker();
        return NYT::NApi::NRpcProxy::CreateConnection(Config_->RpcProxyConnection, std::move(options));
    }

    IConnectionPtr CreateFederatedConnection() const
    {
        YT_VERIFY(Config_->FederatedConnection);
        NYT::NApi::NRpcProxy::TConnectionOptions options;
        options.ConnectionInvoker = Bootstrap_->GetWorkerPoolInvoker();
        return NYT::NClient::NFederated::CreateConnection(Config_->FederatedConnection, std::move(options));
    }

    TClientCachePtr CreateClientCache() const
    {
        if (FederatedConnection_) {
            return New<TClientCache>(Config_->ClientCache, FederatedConnection_);
        }
        YT_VERIFY(Connection_);
        return New<TClientCache>(Config_->ClientCache, Connection_);
    }

    IClientPtr CreateControlClient() const
    {
        YT_VERIFY(Connection_);
        auto options = TClientOptions::FromUser(Config_->User);
        if (auto token = Config_->GetToken()) {
            options.Token = token;
        }
        if (ServiceTicketAuth_) {
            options.User.reset();
            options.ServiceTicketAuth = ServiceTicketAuth_;
        }
        return Connection_->CreateClient(options);
    }

    template <class IClientPtr, class TClientCachePtr>
    IClientPtr GetClientImpl(const TClientCachePtr& clientCache, TString identityUserTag)
    {
        auto identity = NRpc::TAuthenticationIdentity(Config_->User, std::move(identityUserTag));
        auto options = TClientOptions::FromAuthenticationIdentity(identity);
        if (auto token = Config_->GetToken()) {
            options.Token = token;
        }
        if (ServiceTicketAuth_) {
            options.User.reset();
            options.ServiceTicketAuth = ServiceTicketAuth_;
        }
        return clientCache->Get(identity, options);
    }

////////////////////////////////////////////////////////////////////////////////

    IAttributeDictionaryPtr CreateInstanceAttributes()
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("expiration_timeout", Config_->NodeExpirationTimeout);
        attributes->Set("instance_tag", GetInstanceTag());
        attributes->Set("lock_time", TInstant::Now());

        #define XX(name, method) \
            if (auto address = Bootstrap_->Get##method()) { \
                attributes->Set(#name, address); \
            }

        XX(ip6_address, IP6Address);
        XX(client_grpc_address, ClientGrpcAddress);
        XX(client_grpc_ip6_address, ClientGrpcIP6Address);
        XX(secure_client_grpc_address, SecureClientGrpcAddress);
        XX(secure_client_grpc_ip6_address, SecureClientGrpcIP6Address);
        XX(client_http_address, ClientHttpAddress);
        XX(client_http_ip6_address, ClientHttpIP6Address);
        XX(secure_client_http_address, SecureClientHttpAddress);
        XX(secure_client_http_ip6_address, SecureClientHttpIP6Address);
        XX(monitoring_address, MonitoringAddress);
        XX(rpc_proxy_address, RpcProxyAddress);
        XX(rpc_proxy_ip6_address, RpcProxyIP6Address);

        #undef XX

        if (auto dynamicAttributes = Bootstrap_->GetInstanceDynamicAttributes()) {
            attributes->MergeFrom(*dynamicAttributes);
        }
        return attributes;
    }

////////////////////////////////////////////////////////////////////////////////

    void ScheduleConnect(bool immediately = false)
    {
        if (IsConnectScheduled_) {
            return;
        }

        IsConnectScheduled_ = true;
        TDelayedExecutor::Submit(
            BIND(&TImpl::Connect, MakeWeak(this))
                .Via(Bootstrap_->GetControlInvoker()),
            immediately ? TDuration::Zero() : Config_->ReconnectPeriod);
    }

    void Connect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        IsConnectScheduled_ = false;
        try {
            GuardedConnect();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error connecting to YT");
            Disconnect();
        }
    }

    void GuardedConnect()
    {
        auto client = GetControlClient();

        YT_LOG_INFO("Connecting to YT");

        {
            YT_LOG_INFO("Starting instance transaction");

            TTransactionStartOptions options;
            options.Timeout = Config_->InstanceTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Instance transaction for %v", Bootstrap_->GetFqdn()));
            options.Attributes = std::move(attributes);
            InstanceTransaction_ = WaitFor(client->StartTransaction(ETransactionType::Master, options))
                .ValueOrThrow();

            YT_LOG_INFO("Instance transaction started (TransactionId: %v)",
                InstanceTransaction_->GetId());
        }

        InstanceTag_.store(InstanceTagAllocator_->Allocate(
            client,
            InstanceTransaction_,
            Bootstrap_->GetFqdn(),
            Config_->InstanceTag));

        {
            YT_LOG_INFO("Creating instance node");

            TCreateNodeOptions options;
            options.Recursive = true;
            options.Force = true;
            options.PrerequisiteTransactionIds.push_back(InstanceTransaction_->GetId());

            options.Attributes = CreateInstanceAttributes();
            WaitFor(client->CreateNode(
                GetInstancePath(),
                EObjectType::MapNode,
                options))
                .ThrowOnError();

            if (Config_->SetupOrchid) {
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_addresses", Bootstrap_->GetInternalRpcAddresses());
                options.Attributes = std::move(attributes);
                WaitFor(client->CreateNode(
                    GetInstanceOrchidPath(),
                    EObjectType::Orchid,
                    options))
                    .ThrowOnError();
            }
        }

        {
            YT_LOG_INFO("Checking whether instance is banned to forbid connection");

            const auto& fqdn = Bootstrap_->GetFqdn();
            if (GetBannedFqdns(client).contains(fqdn)) {
                THROW_ERROR_EXCEPTION("Instance %v is banned",
                    fqdn);
            }
        }

        {
            YT_LOG_INFO("Locking instance node");

            WaitFor(InstanceTransaction_->LockNode(
                GetInstancePath(),
                ELockMode::Exclusive))
                .ThrowOnError();
        }

        // Sanity-check.
        {
            YT_LOG_INFO("Double-checking instance node instance tag attribute");

            auto actualInstanceTagYson = WaitFor(InstanceTransaction_->GetNode(
                Format("%v/@%v", GetInstancePath(), "instance_tag")))
                .ValueOrThrow();
            auto actualInstanceTag = ConvertTo<TMasterInstanceTag>(actualInstanceTagYson);
            YT_VERIFY(actualInstanceTag == InstanceTag_.load());
        }

        InstanceTransaction_->SubscribeAborted(
            BIND(
                &TImpl::OnInstanceTransactionAborted,
                MakeWeak(this),
                InstanceTransaction_->GetId())
            .Via(Bootstrap_->GetControlInvoker()));

        MasterDiscoveryExecutor_->ScheduleOutOfBand();

        ValidateConnection_.Fire();

        YT_LOG_INFO("YT connected");

        YT_VERIFY(!IsConnected());
        IsConnected_.store(true);
        CancelableContext_ = New<TCancelableContext>();
        CancelableInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker());

        Connected_.Fire();

        ScheduleTakeLeaderLock(true);
    }

    void Disconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        StopLeading();

        if (IsConnected()) {
            YT_LOG_INFO("YT disconnected");
            Disconnected_.Fire();
        }

        if (InstanceTransaction_) {
            // Auto-abort is not supported for all transaction implementations yet,
            // call abort explicitly in a fire-and-forget manner.
            YT_UNUSED_FUTURE(InstanceTransaction_->Abort());
            InstanceTransaction_.Reset();
        }
        if (CancelableContext_) {
            CancelableContext_->Cancel(TError("YT disconnected"));
            CancelableContext_.Reset();
        }
        CancelableInvoker_.Reset();
        IsConnected_.store(false);
        IsLeading_.store(false);
        IsTakeLeaderLockScheduled_ = false;

        ScheduleConnect();
    }

    void OnInstanceTransactionAborted(TTransactionId id, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!InstanceTransaction_ || InstanceTransaction_->GetId() != id) {
            return;
        }

        YT_LOG_WARNING(error, "Instance transaction aborted; disconnecting");
        Disconnect();
    }

////////////////////////////////////////////////////////////////////////////////

    void ScheduleTakeLeaderLock(bool immediately = false)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsTakeLeaderLockScheduled_) {
            return;
        }
        IsTakeLeaderLockScheduled_ = true;
        TDelayedExecutor::Submit(
            BIND(&TImpl::TakeLeaderLock, MakeWeak(this))
                .Via(CancelableInvoker_),
            immediately ? TDuration::Zero() : Config_->ReconnectPeriod);
    }

    void TakeLeaderLock()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        IsTakeLeaderLockScheduled_ = false;
        try {
            GuardedTakeLeaderLock();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Could not take leader lock");
            ScheduleTakeLeaderLock();
        }
    }

    void GuardedTakeLeaderLock()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Trying to take leader lock");

        auto client = GetControlClient();
        const auto& fqdn = Bootstrap_->GetFqdn();

        {
            YT_LOG_DEBUG("Starting leader transaction");

            TTransactionStartOptions options;
            options.Timeout = Config_->LeaderTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Leader transaction for %v", fqdn));
            attributes->Set("fqdn", fqdn);
            options.Attributes = std::move(attributes);
            LeaderTransaction_ = WaitFor(client->StartTransaction(ETransactionType::Master, options))
                .ValueOrThrow();

            YT_LOG_DEBUG("Leader transaction started (TransactionId: %v)",
                LeaderTransaction_->GetId());
        }

        {
            YT_LOG_DEBUG("Taking leader lock");

            auto path = GetLeaderPath();
            WaitFor(LeaderTransaction_->LockNode(path, ELockMode::Exclusive))
                .ThrowOnError();

            YT_LOG_DEBUG("Leader lock taken");
        }

        LeaderTransaction_->SubscribeAborted(
            BIND(
                &TImpl::OnLeaderTransactionAborted,
                MakeWeak(this),
                LeaderTransaction_->GetId())
            .Via(Bootstrap_->GetControlInvoker()));

        YT_LOG_INFO("Started leading");

        YT_VERIFY(!IsLeading());
        IsLeading_.store(true);
        StartedLeading_.Fire();
    }

    void StopLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LeaderTransaction_.Reset();

        if (!IsLeading()) {
            return;
        }

        YT_LOG_INFO("Stopped leading");

        IsLeading_.store(false);
        StoppedLeading_.Fire();
    }

    void OnLeaderTransactionAborted(TTransactionId id, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!LeaderTransaction_ || LeaderTransaction_->GetId() != id) {
            return;
        }

        YT_LOG_WARNING(error, "Leader transaction aborted");

        StopLeading();
        ScheduleTakeLeaderLock();
    }

////////////////////////////////////////////////////////////////////////////////

    void OnMasterDiscovery()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Master discovery started");

        try {
            auto client = GetControlClient();

            std::optional<TString> leaderFqdn;
            {
                auto path = GetLeaderPath() + "/@locks";
                auto yson = WaitFor(client->GetNode(path))
                    .ValueOrThrow();

                auto locks = ConvertTo<IListNodePtr>(yson);
                if (locks->GetChildCount() > 1) {
                    THROW_ERROR_EXCEPTION("More than one leader lock found");
                }

                if (locks->GetChildCount() == 1) {
                    auto transactionId = ConvertTo<TTransactionId>(locks->GetChildOrThrow(0)->AsMap()->GetChildOrThrow("transaction_id"));
                    auto leaderFqdnYson = WaitFor(client->GetNode(Format("%v%v/@fqdn", NObjectClient::ObjectIdPathPrefix, transactionId)))
                        .ValueOrThrow();
                    leaderFqdn = ConvertTo<TString>(leaderFqdnYson);
                    YT_LOG_INFO("Leader discovered (Fqdn: %v)",
                        leaderFqdn);
                }
            }

            TYsonString instancesYson;
            {
                auto path = GetInstancesPath();
                TListNodeOptions options;
                auto attributeKeys = std::vector<TString>{
                    "instance_tag",
                    "lock_time",
                    "ip6_address",
                    "client_grpc_address",
                    "client_grpc_ip6_address",
                    "secure_client_grpc_address",
                    "secure_client_grpc_ip6_address",
                    "client_http_address",
                    "client_http_ip6_address",
                    "secure_client_http_address",
                    "secure_client_http_ip6_address",
                    "rpc_proxy_address",
                    "rpc_proxy_ip6_address",
                    "lock_count"
                };
                attributeKeys.insert(
                    attributeKeys.end(),
                    DynamicAttributeKeys_.begin(),
                    DynamicAttributeKeys_.end());
                options.Attributes = std::move(attributeKeys);
                instancesYson = WaitFor(client->ListNode(path, options))
                    .ValueOrThrow();
            }

            THashMap<TMasterInstanceTag, std::vector<TMasterDiscoveryInfo>> infosPerTag;

            auto instances = ConvertTo<IListNodePtr>(instancesYson);
            auto bannedFqdns = GetBannedFqdns(client);
            for (const auto& child : instances->GetChildren()) {
                TMasterDiscoveryInfo info;

                info.Fqdn = child->GetValue<TString>();
                info.IP6Address = child->Attributes().Get<TString>("ip6_address", TString());
                info.ClientGrpcAddress = child->Attributes().Get<TString>("client_grpc_address", TString());
                info.ClientGrpcIP6Address = child->Attributes().Get<TString>("client_grpc_ip6_address", TString());
                info.SecureClientGrpcAddress = child->Attributes().Get<TString>("secure_client_grpc_address", TString());
                info.SecureClientGrpcIP6Address = child->Attributes().Get<TString>("secure_client_grpc_ip6_address", TString());
                info.ClientHttpAddress = child->Attributes().Get<TString>("client_http_address", TString());
                info.ClientHttpIP6Address = child->Attributes().Get<TString>("client_http_ip6_address", TString());
                info.SecureClientHttpAddress = child->Attributes().Get<TString>("secure_client_http_address", TString());
                info.SecureClientHttpIP6Address = child->Attributes().Get<TString>("secure_client_http_ip6_address", TString());
                info.RpcProxyAddress = child->Attributes().Get<TString>("rpc_proxy_address", TString());
                info.RpcProxyIP6Address = child->Attributes().Get<TString>("rpc_proxy_ip6_address", TString());
                info.InstanceTag = child->Attributes().Get<TMasterInstanceTag>("instance_tag");
                info.LockTime = child->Attributes().Get<TInstant>("lock_time", TInstant::Zero());
                info.Alive = child->Attributes().Get<int>("lock_count") > 0;
                info.Leading = (leaderFqdn && *leaderFqdn == info.Fqdn);

                info.DynamicAttributes = CreateEphemeralAttributes();
                for (const auto& key : DynamicAttributeKeys_) {
                    info.DynamicAttributes->SetYson(
                        key,
                        child->Attributes().GetYson(key));
                }

                if (!IsValidInstanceTag(info.InstanceTag)) {
                    YT_LOG_WARNING("Discovered master with invalid instance tag (Fqdn: %v, InstanceTag: %v)",
                        info.Fqdn,
                        info.InstanceTag);
                    continue;
                }

                if (bannedFqdns.contains(info.Fqdn)) {
                    YT_LOG_WARNING("Discovered banned master (Fqdn: %v)",
                        info.Fqdn);
                    continue;
                }

                if (MasterDiscoveryInfoValidator_) {
                    MasterDiscoveryInfoValidator_(info);
                }

                YT_LOG_DEBUG("Master discovered ("
                    "Fqdn: %v, "
                    "IP6Address: %v, "
                    "ClientGrpcAddress: %v, "
                    "ClientGrpcIP6Address: %v, "
                    "SecureClientGrpcAddress: %v, "
                    "SecureClientGrpcIP6Address: %v, "
                    "ClientHttpAddress: %v, "
                    "ClientHttpIP6Address: %v, "
                    "SecureClientHttpAddress: %v, "
                    "SecureClientHttpIP6Address: %v, "
                    "RpcProxyAddress: %v, "
                    "RpcProxyIP6Address: %v, "
                    "InstanceTag: %v, "
                    "LockTime: %v, "
                    "Alive: %v, "
                    "Leading: %v, "
                    "DynamicAttributes: %v)",
                    info.Fqdn,
                    info.IP6Address,
                    info.ClientGrpcAddress,
                    info.ClientGrpcIP6Address,
                    info.SecureClientGrpcAddress,
                    info.SecureClientGrpcIP6Address,
                    info.ClientHttpAddress,
                    info.ClientHttpIP6Address,
                    info.SecureClientHttpAddress,
                    info.SecureClientHttpIP6Address,
                    info.RpcProxyAddress,
                    info.RpcProxyIP6Address,
                    info.InstanceTag,
                    info.LockTime,
                    info.Alive,
                    info.Leading,
                    ConvertToYsonString(info.DynamicAttributes, NYson::EYsonFormat::Text));

                infosPerTag[info.InstanceTag].push_back(std::move(info));
            }

            // Deduplicate instance tags.
            std::vector<TMasterDiscoveryInfo> infos;
            for (auto& [tag, tagInfos] : infosPerTag) {
                int aliveIndex = -1;
                int lastLockTimeIndex = -1;
                for (int index = 0; index < std::ssize(tagInfos); ++index) {
                    if (tagInfos[index].Alive) {
                        if (aliveIndex >= 0) {
                            YT_LOG_WARNING("Discovered several alive instances with the same instance tag (Fqdn1: %v, Fqdn2: %v, Tag: %v)",
                                tagInfos[aliveIndex].Fqdn,
                                tagInfos[index].Fqdn,
                                tag);
                        } else {
                            aliveIndex = index;
                        }
                    }
                    if (lastLockTimeIndex == -1 || tagInfos[index].LockTime > tagInfos[lastLockTimeIndex].LockTime) {
                        lastLockTimeIndex = index;
                    }
                }

                int pickedIndex = aliveIndex >= 0 ? aliveIndex : lastLockTimeIndex;
                YT_VERIFY(pickedIndex >= 0);

                for (int index = 0; index < std::ssize(tagInfos); ++index) {
                    if (index != pickedIndex) {
                        YT_LOG_DEBUG("Filtered instance with a duplicate instance tag (Fqdn: %v, Tag: %v)",
                            tagInfos[index].Fqdn,
                            tag);
                    }
                }

                infos.push_back(std::move(tagInfos[pickedIndex]));
            }

            MasterDiscoveryInfos_.Store(std::move(infos));

            YT_LOG_INFO("Master discovery completed");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Master discovery failed");
        }
    }

////////////////////////////////////////////////////////////////////////////////

    THashSet<TString> GetBannedFqdns(IClientPtr client)
    {
        auto ysonOrError = WaitFor(client->ListNode(
            GetBannedInstancesByFqdnPath()));
        if (!ysonOrError.IsOK()) {
            if (ysonOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                return {};
            } else {
                THROW_ERROR_EXCEPTION("Error getting instances banned by fqdn") << ysonOrError;
            }
        }
        return ConvertTo<THashSet<TString>>(ysonOrError.Value());
    }

    void OnBanCheck()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Ban check started");

        try {
            auto client = GetControlClient();
            const auto& fqdn = Bootstrap_->GetFqdn();
            if (GetBannedFqdns(client).contains(fqdn) && InstanceTransaction_) {
                YT_LOG_INFO("Instance is banned, aborting the instance transaction");
                // Do not reset instance transaction here, as there may be a race:
                // 1. Instance transaction is created.
                // 2. Ban checker bans transaction and resets transaction.
                // 3. SubscribeAborted calls OnInstanceTransactionAborted immediately, does not call Disconnect, as transaction is already reset.
                YT_UNUSED_FUTURE(InstanceTransaction_->Abort());
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Ban check failed");
        }
    }

////////////////////////////////////////////////////////////////////////////////

    TYPath GetInstancesPath()
    {
        return MasterPath_ + "/instances";
    }

    TYPath GetInstanceTagsPath()
    {
        return MasterPath_ + "/instance_tags";
    }

    TYPath GetInstancePath()
    {
        return GetInstancesPath() + "/" + ToYPathLiteral(Bootstrap_->GetFqdn());
    }

    TYPath GetInstanceOrchidPath()
    {
        return GetInstancePath() + "/orchid";
    }

    TYPath GetBannedInstancesByFqdnPath()
    {
        return MasterPath_ + "/instances_banned_by_fqdn";
    }

    TYPath GetLeaderPath()
    {
        return MasterPath_ + "/leader";
    }
};

////////////////////////////////////////////////////////////////////////////////

TYTConnector::TYTConnector(
    IBootstrap* bootstrap,
    TYTConnectorConfigPtr config,
    int expectedDBVersion,
    std::vector<TString> dynamicAttributeKeys,
    TMasterDiscoveryInfoValidator masterDiscoveryInfoValidator)
    : Impl_(New<TImpl>(
        bootstrap,
        std::move(config),
        expectedDBVersion,
        std::move(dynamicAttributeKeys),
        std::move(masterDiscoveryInfoValidator)))
{ }

TYTConnector::~TYTConnector() = default;

void TYTConnector::Initialize()
{
    Impl_->Initialize();
}

void TYTConnector::Start()
{
    Impl_->Start();
}

IClientPtr TYTConnector::GetControlClient()
{
    return Impl_->GetControlClient();
}

std::string TYTConnector::FormatUserTag(std::string userTag) const
{
    return Impl_->FormatUserTag(std::move(userTag));
}

IClientPtr TYTConnector::GetClient(std::string identityUserTag)
{
    return Impl_->GetClient(std::move(identityUserTag));
}

NNative::IClientPtr TYTConnector::GetNativeClient(std::string identityUserTag)
{
    return Impl_->GetNativeClient(std::move(identityUserTag));
}

NNative::IConnectionPtr TYTConnector::GetNativeConnection()
{
    return Impl_->GetNativeConnection();
}

const TYPath& TYTConnector::GetRootPath()
{
    return Impl_->GetRootPath();
}

const TYPath& TYTConnector::GetDBPath()
{
    return Impl_->GetDBPath();
}

const TYPath& TYTConnector::GetMasterPath()
{
    return Impl_->GetMasterPath();
}

TYPath TYTConnector::GetConsumerPath(std::string_view consumer)
{
    return Impl_->GetConsumerPath(consumer);
}

TYPath TYTConnector::GetTablePath(const NObjects::TDBTable* table)
{
    return Impl_->GetTablePath(table);
}

TClusterTag TYTConnector::GetClusterTag()
{
    return Impl_->GetClusterTag();
}

const std::string& TYTConnector::GetClusterName()
{
    return Impl_->GetClusterName();
}

TMasterInstanceTag TYTConnector::GetInstanceTag()
{
    return Impl_->GetInstanceTag();
}

const TYTConnectorConfigPtr& TYTConnector::GetConfig()
{
    return Impl_->GetConfig();
}

bool TYTConnector::IsConnected() const
{
    return Impl_->IsConnected();
}

TError TYTConnector::CheckConnected() const
{
    return Impl_->CheckConnected();
}

bool TYTConnector::IsLeading() const
{
    return Impl_->IsLeading();
}

const ITransactionPtr& TYTConnector::GetInstanceTransaction()
{
    return Impl_->GetInstanceTransaction();
}

int TYTConnector::GetExpectedDBVersion() const
{
    return Impl_->GetExpectedDBVersion();
}

std::vector<TMasterDiscoveryInfo> TYTConnector::GetMasters()
{
    return Impl_->GetMasters();
}

DELEGATE_SIGNAL(TYTConnector, void(), Connected, *Impl_);
DELEGATE_SIGNAL(TYTConnector, void(), Disconnected, *Impl_);
DELEGATE_SIGNAL(TYTConnector, void(), ValidateConnection, *Impl_);

DELEGATE_SIGNAL(TYTConnector, void(), StartedLeading, *Impl_);
DELEGATE_SIGNAL(TYTConnector, void(), StoppedLeading, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
