#include "coordination_helper.h"
#include "global_worker_manager.h"
#include "service_node_pinger.h"

#include <yt/yql/providers/dq/actors/yt/nodeid_assigner.h>
#include <yt/yql/providers/dq/actors/yt/nodeid_cleaner.h>
#include <yt/yql/providers/dq/actors/yt/worker_registrator.h>
#include <yt/yql/providers/dq/actors/yt/lock.h>
#include <yt/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <yt/yql/providers/dq/actors/dummy_lock.h>
#include <yt/yql/providers/dq/service/interconnect_helpers.h>
#include <contrib/ydb/library/yql/providers/dq/task_runner/file_cache.h>
#include <contrib/ydb/library/yql/providers/dq/runtime/runtime_data.h>

#include <yql/essentials/utils/log/log.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

#include <util/system/getpid.h>
#include <util/system/env.h>

namespace NYql {

using namespace NActors;

class TCoordinationHelper: public ICoordinationHelper
{
public:
    TCoordinationHelper(
        const TMaybe<NProto::TDqConfig::TYtCoordinator>& config,
        const NProto::TDqConfig::TScheduler& schedulerConfig,
        const TString& role,
        ui16 interconnectPort,
        const TString& hostName,
        const TString& ip,
        TGlobalWorkerManagerActorIdOptions actorIdOptions)
        : Config(config), SchedulerConfig(schedulerConfig)
        , ActorIdOptions(actorIdOptions)
        , Role(role)
        , Host(hostName)
        , Ip(ip)
        , InterconnectPort(interconnectPort)
    { }

    ui32 GetNodeId() override
    {
        Y_ABORT_UNLESS(Config);
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));
        return NodeId;
    }

    ui32 GetNodeId(
        const TMaybe<ui32> nodeId,
        const TMaybe<TString>& grpcPort,
        ui32 minNodeId,
        ui32 maxNodeId,
        const THashMap<TString, TString>& attributes) override
    {
        Y_ABORT_UNLESS(Config);
        if (grpcPort) {
            GrpcPort = *grpcPort;
        }

        if (nodeId) {
            NodeId = *nodeId;
        }

        if (NodeId != static_cast<ui32>(-1)) {
            return NodeId;
        }

        const auto& config = GetConfig();
        NYql::TAssignNodeIdOptions options;

        options.ProxyAddress = config.GetProxyAddress();
        options.User = config.GetUser();
        options.Token = config.GetToken();
        options.Prefix = config.GetPrefix();
        options.Role = Role;
        options.Attributes[NCommonAttrs::ROLE_ATTR] = Role;
        if (grpcPort) {
            options.Attributes[NCommonAttrs::GRPCPORT_ATTR] = *grpcPort;
        }
        Y_ABORT_UNLESS(InterconnectPort);
        options.Attributes[NCommonAttrs::INTERCONNECTPORT_ATTR] = ToString(InterconnectPort);
        options.Attributes[NCommonAttrs::HOSTNAME_ATTR] = Host;
        options.Attributes[NCommonAttrs::REVISION_ATTR] = GetRevision();

        options.NodeName = TStringBuilder() << Host << ":" << GetPID() << ":" << InterconnectPort;
        options.NodeId = nodeId;
        options.MinNodeId = minNodeId;
        options.MaxNodeId = maxNodeId;

        NodeName = options.NodeName;

        for (const auto& [k, v]: attributes) {
            options.Attributes[k] = v;
        }

        NodeId = AssignNodeId(options);
        return NodeId;
    }

    NActors::IActor* CreateLockOnCluster(NActors::TActorId ytWrapper, const TString& prefix, const TString& lockName, bool temporary) override {
        Y_ABORT_UNLESS(!Config || NodeId != static_cast<ui32>(-1));

        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
                .Item(NCommonAttrs::HOSTNAME_ATTR).Value(GetHostname())
                .Item(NCommonAttrs::GRPCPORT_ATTR).Value(GrpcPort) // optional
                .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true") // compat
            .EndMap();
        if (NodeId != static_cast<ui32>(-1)) {
            attributes[NCommonAttrs::ACTOR_NODEID_ATTR] = NodeId;
        }

        return CreateYtLock(
                ytWrapper,
                prefix + "/locks",
                lockName,
                NYT::NodeToYsonString(attributes),
                temporary);
    }

    NActors::IActor* CreateLock(const TString& lockName, bool temporary) override {
        const auto& config = GetConfig();
        YQL_CLOG(DEBUG, ProviderDq) << "CreateLock"
            << " name=" << lockName
            << " prefix=" << config.GetPrefix()
            << " cluster=" << config.GetClusterName()
            << " user=" << config.GetUser()
            << " has_token=" << !config.GetToken().empty()
            << " yt_wrapper=" << GetWrapper()
            << " grpc_port=" << GrpcPort;
        return CreateLockOnCluster(GetWrapper(), config.GetPrefix(), lockName, temporary);
    }

    void StartRegistrator(NActors::TActorSystem* actorSystem) override {
        const auto& config = GetConfig();
        TWorkerRegistratorOptions wro;
        wro.NodeName = NodeName;
        wro.Prefix = config.GetPrefix() + "/" + Role;
        YQL_CLOG(DEBUG, ProviderDq) << "Start service node registrator"
            << " ytPath=" << wro.Prefix << "/" << wro.NodeName
            << " nodeId=" << NodeId
            << " role=" << Role;
        Register(actorSystem, CreateWorkerRegistrator(GetWrapper(actorSystem), wro));
    }

    void StartCleaner(NActors::TActorSystem* actorSystem, const TMaybe<TString>& role) override {
        const auto& config = GetConfig();
        TNodeIdCleanerOptions co;
        co.Prefix = config.GetPrefix() + "/" + role.GetOrElse(Role);
        Register(actorSystem, CreateNodeIdCleaner(GetWrapper(actorSystem), co));
    }

    TString GetHostname() override {
        return Host;
    }

    TString GetIp() override {
        return Ip;
    }

    IServiceNodeResolver::TPtr CreateServiceNodeResolver(
        NActors::TActorSystem* actorSystem,
        const TVector<TString>& hostPortPairs) override
    {
        if (!hostPortPairs.empty()) {
            return CreateStaticResolver(hostPortPairs);
        } else {
            const auto& config = GetConfig();
            TDynamicResolverOptions options;
            options.YtWrapper = GetWrapper(actorSystem);
            options.Prefix = config.GetPrefix() + "/service_node";
            YQL_CLOG(DEBUG, ProviderDq) << "Create dynamic service node resolver"
                << " prefix=" << options.Prefix;
            return CreateDynamicResolver(actorSystem, options);
        }
    }

    void StartGlobalWorker(
        NActors::TActorSystem* actorSystem,
        const TVector<TResourceManagerOptions>& resourceUploaderOptions,
        IMetricsRegistryPtr metricsRegistry) override
    {
        Y_ABORT_UNLESS(ActorIdOptions.RegisterLegacyActorId || ActorIdOptions.UseGlobalActorId);
        const auto& config = GetConfig();
        if (config.GetLockType() != "dummy") {
            GetWrapper();
        }
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));
        auto actorId = Register(actorSystem, CreateGlobalWorkerManager(
            this,
            resourceUploaderOptions,
            std::move(metricsRegistry),
            SchedulerConfig,
            /*scheduleInterval*/ TDuration::MilliSeconds(100),
            ActorIdOptions));
        const auto legacyServiceActorId = NDqs::MakeWorkerManagerActorID(NodeId);
        const auto globalServiceActorId = NDqs::MakeGlobalWorkerManagerActorID(NodeId);
        YQL_CLOG(INFO, ProviderDq) << "StartGlobalWorker: legacyServiceActorId: " << legacyServiceActorId
            << ", globalServiceActorId: " << globalServiceActorId << ", actorId: " << actorId << ", NodeId: " << NodeId
            << ", registerLegacyActorId: " << ActorIdOptions.RegisterLegacyActorId
            << ", useGlobalActorId: " << ActorIdOptions.UseGlobalActorId;
        if (ActorIdOptions.RegisterLegacyActorId) {
            actorSystem->RegisterLocalService(legacyServiceActorId, actorId);
        }
        actorSystem->RegisterLocalService(globalServiceActorId, actorId);
    }

    const NProto::TDqConfig::TYtCoordinator& GetConfig() override {
        Y_ABORT_UNLESS(Config, "Coordinator configuration is required for this operation");
        return *Config;
    }

    NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem, const TString& proxyAddress, const TString& user, const TString& token) override {
        auto key = std::make_tuple(proxyAddress, user, token);
        auto guard = Guard(Mutex);
        auto it = Yt.find(key);
        if (it != Yt.end()) {
            return it->second;
        } else {
            auto client = GetYtClient(proxyAddress, user, token);
            auto wrapper = CreateYtWrapper(client, proxyAddress);
            auto actorId = Register(actorSystem, wrapper);
            Yt.emplace(key, actorId);
            YQL_CLOG(DEBUG, ProviderDq) << "Registered YtWrapper"
                << " cluster=" << proxyAddress
                << " user=" << user
                << " actor_id=" << actorId;
            return actorId;
        }
    }

    NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem) override {
        const auto& config = GetConfig();
        return GetWrapper(actorSystem, config.GetProxyAddress(), config.GetUser(), config.GetToken());
    }

    NActors::TActorId GetWrapper() override {
        const auto& config = GetConfig();
        auto key = std::make_tuple(config.GetProxyAddress(), config.GetUser(), config.GetToken());
        auto guard = Guard(Mutex);
        auto it = Yt.find(key);
        Y_ABORT_UNLESS(it != Yt.end());
        return it->second;
    }

    NActors::IActor* CreateServiceNodePinger(
        const IServiceNodeResolver::TPtr& ptr,
        const TResourceManagerOptions& rmOptions,
        const THashMap<TString, TString>& attributes) override
    {
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));
        Y_ABORT_UNLESS(InterconnectPort);
        return ::NYql::CreateServiceNodePinger(
            NodeId,
            Ip,
            InterconnectPort,
            Role,
            attributes,
            ptr,
            this,
            rmOptions,
            ActorIdOptions);
    }

    TWorkerRuntimeData* GetRuntimeData() override {
        return &RuntimeData;
    }

    void Stop(NActors::TActorSystem* actorSystem) override {
        for (auto id : Children) {
            actorSystem->Send(id, new TEvents::TEvPoison());
        }
        Children.clear();
    }

    TString GetRevision() override {
        if (Config && Config->HasRevision()) {
            return Config->GetRevision();
        } else {
            return GetProgramCommitId();
        }
    }

protected:
    TActorId Register(TActorSystem* actorSystem, IActor* actor) {
        auto id = actorSystem->Register(actor);
        Children.push_back(id);
        return id;
    }

    NYT::NApi::IClientPtr GetYtClient(const TString& proxyAddress, const TString& user, const TString& token)
    {
        NYT::NApi::NRpcProxy::TConnectionConfigPtr config = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
        config->RequestCodec = NYT::NCompression::ECodec::Lz4;
        config->ClusterUrl = proxyAddress;
        config->ConnectionType = NYT::NApi::EConnectionType::Rpc;

        auto connection = NYT::NApi::NRpcProxy::CreateConnection(config);

        NYT::NApi::TClientOptions options;
        options.User = user;
        options.Token = token;

        auto client = connection->CreateClient(options);
        return client;
    }

    NYT::NApi::IClientPtr GetYtClient()
    {
        const auto& config = GetConfig();
        return GetYtClient(config.GetProxyAddress(), config.GetUser(), config.GetToken());
    }

    const TMaybe<NProto::TDqConfig::TYtCoordinator> Config;
    const NProto::TDqConfig::TScheduler SchedulerConfig;
    const TGlobalWorkerManagerActorIdOptions ActorIdOptions;

    TString Role;

    TString Host;
    TString Ip;

    TString NodeName;

    TMutex Mutex;
    THashMap<std::tuple<TString, TString, TString>, TActorId> Yt;

    ui32 NodeId = -1;
    TString GrpcPort = "";
    ui16 InterconnectPort;

    TWorkerRuntimeData RuntimeData;
    IFileCache::TPtr FileCache;

    TVector<TActorId> Children;
};

class TCoordinationHelperWithDummyLock: public TCoordinationHelper
{
public:
    TCoordinationHelperWithDummyLock(
        const NProto::TDqConfig::TYtCoordinator& config,
        const NProto::TDqConfig::TScheduler& schedulerConfig,
        const TString& role,
        ui16 interconnectPort,
        const TString& host,
        const TString& ip,
        TGlobalWorkerManagerActorIdOptions actorIdOptions)
        : TCoordinationHelper(MakeMaybe(config), schedulerConfig, role, interconnectPort, host, ip, actorIdOptions)
    { }

    NActors::IActor* CreateLockOnCluster(NActors::TActorId ytWrapper, const TString& prefix, const TString& lockName, bool temporary) override {
        Y_UNUSED(ytWrapper);
        Y_UNUSED(prefix);
        Y_UNUSED(temporary);

        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));

        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
                .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(NodeId)
                .Item(NCommonAttrs::HOSTNAME_ATTR).Value(GetHostname())
                .Item(NCommonAttrs::GRPCPORT_ATTR).Value(GrpcPort) // optional
                .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true") // compat
            .EndMap();

        return CreateDummyLock(
                lockName,
                NYT::NodeToYsonString(attributes));
    }

    NActors::IActor* CreateLock(const TString& lockName, bool temporary) override {
        const auto& config = GetConfig();
        return CreateLockOnCluster(NActors::TActorId(), config.GetPrefix(), lockName, temporary);
    }
};

ICoordinationHelper::TPtr CreateUploadCoordinationHelper(const TString& hostName)
{
    return new TCoordinationHelper(
        /*config*/ Nothing(),
        /*schedulerConfig*/ {},
        /*role*/ "resource_uploader",
        /*interconnectPort*/ 0,
        hostName,
        /*ip*/ {},
        /*actorIdOptions*/ {});
}

ICoordinationHelper::TPtr CreateCoordiantionHelper(
    const NProto::TDqConfig::TYtCoordinator& cfg,
    const NProto::TDqConfig::TScheduler& schedulerConfig,
    const TString& role,
    ui16 interconnectPort,
    const TString& host,
    const TString& ip,
    TGlobalWorkerManagerActorIdOptions actorIdOptions)
{
    NProto::TDqConfig::TYtCoordinator config = cfg;
    const auto proxyAddress = config.GetProxyAddress();

    TString userName;
    TString token;

    if (config.HasToken()) {
        // internal job
        userName = config.GetUser();
        token = config.GetToken();
    } else if (!proxyAddress.empty()) {
        std::tie(userName, token) = NDqs::GetUserToken(
            config.HasUser() ? TMaybe<TString>(config.GetUser()) : TMaybe<TString>(),
            config.HasTokenFile() ? TMaybe<TString>(config.GetTokenFile()) : TMaybe<TString>()
        );
    }

    auto prefix = config.GetPrefix();

    if (config.GetLockType() != "dummy") {
        Y_ABORT_UNLESS(!token.empty());
        Y_ABORT_UNLESS(!proxyAddress.empty());
        Y_ABORT_UNLESS(!userName.empty());
        Y_ABORT_UNLESS(!prefix.empty());
    }

    Y_ABORT_UNLESS(!role.empty());

    config.SetUser(userName);
    config.SetToken(token);

    if (config.GetLockType() == "dummy") {
        return new TCoordinationHelperWithDummyLock(config, schedulerConfig, role, interconnectPort, host, ip, actorIdOptions);
    } else {
        return new TCoordinationHelper(MakeMaybe(config), schedulerConfig, role, interconnectPort, host, ip, actorIdOptions);
    }
}

} // namespace NYql
