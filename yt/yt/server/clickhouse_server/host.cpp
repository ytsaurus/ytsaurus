#include "host.h"

#include "clickhouse_invoker.h"
#include "clickhouse_service_proxy.h"
#include "data_type_boolean.h"
#include "query_context.h"
#include "query_registry.h"
#include "poco_config.h"
#include "config.h"
#include "storage_distributor.h"
#include "storage_system_clique.h"
#include "yt/server/clickhouse_server/private.h"
#include "yt_database.h"
#include "table_functions.h"
#include "table_functions_concat.h"
#include "dictionary_source.h"
#include "memory_watchdog.h"
#include "health_checker.h"

#include <yt/server/lib/misc/address_helpers.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/ytlib/table_client/table_columnar_statistics_cache.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/ytlib/security_client/permission_cache.h>

#include <yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/client/misc/discovery.h>

#include <yt/yt/library/clickhouse_functions/functions.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/crash_handler.h>

#include <yt/core/net/local_address.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/caching_channel_factory.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/HTTPCommon.h>

#include <common/DateLUT.h>

#include <util/system/env.h>

#include <atomic>
#include <memory>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NApi::NNative;
using namespace NProfiling;
using namespace NYTree;
using namespace NRpc::NBus;
using namespace NProto;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NTracing;
using namespace NChunkClient;
using namespace NConcurrency;

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

static const std::vector<TString> AttributesToCache{
    "id",
    "schema",
    "type",
    "dynamic",
    "chunk_count",
    "external",
    "external_cell_tag",
    "partitions",
    "partitioned_by",
    "boundary_keys",
    "enable_dynamic_store_read",
};

static const std::vector<TString> DiscoveryAttributes{
    "host",
    "rpc_port",
    "monitoring_port",
    "tcp_port",
    "http_port",
    "pid",
    "job_cookie",
};

static const TString SysClickHouse = "//sys/clickhouse";

///////////////////////////////////////////////////////////////////////////////

class THost::TImpl
    : public TRefCounted
{
public:
    TImpl(
        THost* owner,
        IInvokerPtr controlInvoker,
        TYtConfigPtr config,
        NApi::NNative::TConnectionConfigPtr connectionConfig,
        TPorts ports)
        : Owner_(owner)
        , ControlInvoker_(std::move(controlInvoker))
        , Config_(std::move(config))
        , Ports_(ports)
        , ConnectionConfig_(std::move(connectionConfig))
        , GossipExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TImpl::MakeGossip, MakeWeak(this)),
            Config_->GossipPeriod))
        , WorkerThreadPool_(New<TThreadPool>(Config_->WorkerThreadCount, "Worker"))
        , WorkerInvoker_(WorkerThreadPool_->GetInvoker())
        , ClickHouseWorkerInvoker_(CreateClickHouseInvoker(WorkerInvoker_))
        , FetcherThreadPool_(New<TThreadPool>(Config_->FetcherThreadCount, "Fetcher"))
        , FetcherInvoker_(FetcherThreadPool_->GetInvoker())
        , ClickHouseFetcherInvoker_(CreateClickHouseInvoker(FetcherInvoker_))
    {
        InitializeClients();
        InitializeCaches();
        InitializeReaderMemoryManager();
        RegisterFactories();

        // Configure clique's directory.
        Config_->Discovery->Directory += "/" + ToString(Config_->CliqueId);

        Discovery_ = New<TDiscovery>(
            Config_->Discovery,
            RootClient_,
            ControlInvoker_,
            DiscoveryAttributes,
            Logger);

        if (Config_->CpuLimit) {
            ClickHouseYtProfiler.AddFuncGauge(
                "/cpu_limit",
                MakeStrong(this),
                [this] {
                    return *Config_->CpuLimit;
                });
        }

        ClickHouseYtProfiler.AddFuncGauge(
            "/memory_limit/watchdog",
            MakeStrong(this),
            [this] {
                return Config_->MemoryWatchdog->MemoryLimit - Config_->MemoryWatchdog->CodicilWatermark;
            });

        ClickHouseYtProfiler.AddFuncGauge(
            "/memory_limit/oom",
            MakeStrong(this),
            [this] {
                return Config_->MemoryWatchdog->MemoryLimit;
            });
    }

    void SetContext(DB::Context* context)
    {
        YT_VERIFY(context && !Context_);
        Context_ = context;

        QueryRegistry_ = New<TQueryRegistry>(
            ControlInvoker_,
            Context_,
            Config_->ProcessListSnapshotUpdatePeriod);
        MemoryWatchdog_ = New<TMemoryWatchdog>(
            Config_->MemoryWatchdog,
            BIND(&TQueryRegistry::WriteStateToStderr, QueryRegistry_),
            BIND([] { raise(SIGINT); }));
        HealthChecker_ = New<THealthChecker>(
            Config_->HealthChecker,
            Config_->User,
            Context_,
            Owner_);
    }

    void Start()
    {
        VERIFY_INVOKER_AFFINITY(GetControlInvoker());

        YT_VERIFY(Context_);

        QueryRegistry_->Start();
        MemoryWatchdog_->Start();

        GossipExecutor_->Start();
        HealthChecker_->Start();
        CreateOrchidNode();
        StartDiscovery();

        WriteToStderr("*** Serving started ***\n");
    }

    void HandleIncomingGossip(const TString& instanceId, EInstanceState state)
    {
        BIND(&TImpl::DoHandleIncomingGossip, MakeWeak(this), instanceId, state)
            .Via(ControlInvoker_)
            .Run();
    }

    TFuture<void> StopDiscovery()
    {
        GossipExecutor_->ScheduleOutOfBand();
        return Discovery_->Leave();
    }

    void ValidateReadPermissions(
        const std::vector<NYPath::TRichYPath>& paths,
        const TString& user)
    {
        std::vector<TPermissionKey> permissionCacheKeys;
        permissionCacheKeys.reserve(paths.size());
        for (const auto& path : paths) {
            permissionCacheKeys.push_back(TPermissionKey{
                .Object = path.GetPath(),
                .User = user,
                .Permission = EPermission::Read,
                .Columns = path.GetColumns()
            });
        }
        auto validationResults = WaitFor(PermissionCache_->Get(permissionCacheKeys))
            .ValueOrThrow();

        std::vector<TError> errors;
        for (size_t index = 0; index < validationResults.size(); ++index) {
            const auto& validationResult = validationResults[index];
            PermissionCache_->Set(permissionCacheKeys[index], validationResult);

            if (!validationResult.IsOK()) {
                errors.push_back(validationResult
                    << TErrorAttribute("path", paths[index])
                    << TErrorAttribute("permission", "read")
                    << TErrorAttribute("columns", paths[index].GetColumns()));
            }
        }
        if (!errors.empty()) {
            constexpr int MaxInnerErrorCount = 10;
            if (errors.size() > MaxInnerErrorCount) {
                errors.resize(MaxInnerErrorCount);
            }

            THROW_ERROR_EXCEPTION("Error validating permissions for user %Qv", user) << errors;
        }
    }

    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> GetObjectAttributes(
        const std::vector<TYPath>& paths,
        const IClientPtr& client)
    {
        const auto& user = client->GetOptions().GetAuthenticatedUser();
        auto cachedAttributes = TableAttributeCache_->Find(paths);
        std::vector<TYPath> missedPaths;
        for (int index = 0; index < (int)paths.size(); ++index) {
            if (!cachedAttributes[index].has_value()) {
                missedPaths.push_back(paths[index]);
            }
        }

        YT_LOG_DEBUG("Getting object attributes (HitCount: %v, MissedCount: %v, User: %v)",
            paths.size() - missedPaths.size(),
            missedPaths.size(),
            user);

        std::reverse(missedPaths.begin(), missedPaths.end());

        // TODO(max42): eliminate this.
        auto attributesForMissedPaths = WaitFor(TableAttributeCache_->GetFromClient(
            missedPaths,
            client,
            GetCurrentInvoker(),
            AttributesToCache,
            Logger,
            Config_->TableAttributeCache->GetMasterReadOptions()))
            .ValueOrThrow();

        std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> attributes;
        attributes.reserve(paths.size());

        for (auto& cachedAttributeOrError : cachedAttributes) {
            if (cachedAttributeOrError.has_value()) {
                attributes.emplace_back(std::move(*cachedAttributeOrError));
            } else {
                TableAttributeCache_->Set(missedPaths.back(), attributesForMissedPaths.back());
                attributes.emplace_back(std::move(attributesForMissedPaths.back()));
                attributesForMissedPaths.pop_back();
                missedPaths.pop_back();
            }
        }

        return attributes;
    }

    const TObjectAttributeCachePtr& GetObjectAttributeCache() const
    {
        return TableAttributeCache_;
    }

    TClusterNodes GetNodes() const
    {
        auto nodeList = Discovery_->List();
        TClusterNodes result;
        result.reserve(nodeList.size());
        for (const auto& [_, attributes] : nodeList) {
            auto host = attributes->Get<TString>("host");
            auto tcpPort = attributes->Get<i64>("tcp_port");
            result.push_back(CreateClusterNode(TClusterNodeName{host, tcpPort}, Context_->getSettingsRef()));
        }
        return result;
    }

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlInvoker_;
    }

    const IInvokerPtr& GetWorkerInvoker() const
    {
        return WorkerInvoker_;
    }

    const IInvokerPtr& GetClickHouseWorkerInvoker() const
    {
        return ClickHouseWorkerInvoker_;
    }

    const IInvokerPtr& GetFetcherInvoker() const
    {
        return FetcherInvoker_;
    }

    const IInvokerPtr& GetClickHouseFetcherInvoker() const
    {
        return ClickHouseFetcherInvoker_;
    }

    const IMultiReaderMemoryManagerPtr& GetMultiReaderMemoryManager() const
    {
        return ParallelReaderMemoryManager_;
    }

    void HandleCrashSignal() const
    {
        QueryRegistry_->WriteStateToStderr();
        WriteToStderr("*** Current query id (possible reason of failure): ");
        const auto& queryId = DB::CurrentThread::getQueryId();
        WriteToStderr(queryId.data, queryId.size);
        WriteToStderr(" ***\n");
    }

    TFuture<void> GetIdleFuture() const
    {
        return QueryRegistry_->GetIdleFuture();
    }

    NApi::NNative::IClientPtr GetRootClient() const
    {
        return RootClient_;
    }

    NApi::NNative::IClientPtr CreateClient(const TString& user)
    {
        auto identity = NRpc::TAuthenticationIdentity(user);
        auto options = NApi::TClientOptions::FromAuthenticationIdentity(identity);
        return ClientCache_->Get(identity, options);
    }

    void HandleSigint()
    {
        ++SigintCounter_;
    }

    TQueryRegistryPtr GetQueryRegistry() const
    {
        return QueryRegistry_;
    }

    void SaveQueryRegistryState()
    {
        WaitFor(
            BIND(&TQueryRegistry::SaveState, QueryRegistry_)
                .AsyncVia(ControlInvoker_)
                .Run())
            .ThrowOnError();
    }

    TYtConfigPtr GetConfig() const
    {
        return Config_;
    }

    EInstanceState GetInstanceState() const
    {
        return SigintCounter_ == 0 ? EInstanceState::Active : EInstanceState::Stopped;
    }

    void PopulateSystemDatabase(DB::IDatabase* systemDatabase) const
    {
        systemDatabase->attachTable("clique", CreateStorageSystemClique(Discovery_, Config_->InstanceId));
    }

    std::shared_ptr<DB::IDatabase> CreateYtDatabase() const
    {
        return NYT::NClickHouseServer::CreateYtDatabase();
    }

    NTableClient::TTableColumnarStatisticsCachePtr GetTableColumnarStatisticsCache() const
    {
        return TableColumnarStatisticsCache_;
    }

private:
    THost* Owner_;
    DB::Context* Context_ = nullptr;
    const IInvokerPtr ControlInvoker_;
    const TYtConfigPtr Config_;
    TPorts Ports_;
    const NApi::NNative::TConnectionConfigPtr ConnectionConfig_;
    THealthCheckerPtr HealthChecker_;
    TMemoryWatchdogPtr MemoryWatchdog_;
    TQueryRegistryPtr QueryRegistry_;
    TPeriodicExecutorPtr GossipExecutor_;
    NConcurrency::TThreadPoolPtr WorkerThreadPool_;
    IInvokerPtr WorkerInvoker_;
    IInvokerPtr ClickHouseWorkerInvoker_;
    NConcurrency::TThreadPoolPtr FetcherThreadPool_;
    IInvokerPtr FetcherInvoker_;
    IInvokerPtr ClickHouseFetcherInvoker_;

    NApi::NNative::IClientPtr RootClient_;
    NApi::NNative::IClientPtr CacheClient_;
    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::TClientCachePtr ClientCache_;

    TPermissionCachePtr PermissionCache_;
    TObjectAttributeCachePtr TableAttributeCache_;
    NTableClient::TTableColumnarStatisticsCachePtr TableColumnarStatisticsCache_;

    TDiscoveryPtr Discovery_;

    NRpc::IChannelFactoryPtr ChannelFactory_;

    THashSet<TString> KnownInstances_;
    THashMap<TString, int> UnknownInstancePingCounter_;

    IMultiReaderMemoryManagerPtr ParallelReaderMemoryManager_;

    std::atomic<int> SigintCounter_ = {0};

    void InitializeClients()
    {
        NApi::NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;

        ChannelFactory_ = CreateCachingChannelFactory(CreateBusChannelFactory(New<NBus::TTcpBusConfig>()));

        Connection_ = NApi::NNative::CreateConnection(
            ConnectionConfig_,
            connectionOptions);

        // Kick-start node directory synchronizing; otherwise it will start only with first query.
        Connection_->GetNodeDirectorySynchronizer()->Start();

        ClientCache_ = New<NApi::NNative::TClientCache>(Config_->ClientCache, Connection_);

        auto getClientForUser = [&] (const TString& user) {
            auto identity = NRpc::TAuthenticationIdentity(user);
            auto options = NApi::TClientOptions::FromAuthenticationIdentity(identity);
            return ClientCache_->Get(identity, options);
        };
        RootClient_ = getClientForUser(Config_->User);
        CacheClient_ = getClientForUser(CacheUserName);
    }

    void InitializeCaches()
    {
        PermissionCache_ = New<TPermissionCache>(
            Config_->PermissionCache,
            Connection_,
            ClickHouseYtProfiler.WithPrefix("/permission_cache"));

        TableAttributeCache_ = New<NObjectClient::TObjectAttributeCache>(
            Config_->TableAttributeCache,
            AttributesToCache,
            CacheClient_,
            ControlInvoker_,
            Logger,
            ClickHouseYtProfiler.WithPrefix("/object_attribute_cache"));

        TableColumnarStatisticsCache_ = New<NTableClient::TTableColumnarStatisticsCache>(
            Config_->TableColumnarStatisticsCache,
            CacheClient_,
            FetcherInvoker_,
            Logger,
            ClickHouseYtProfiler.WithPrefix("/table_columnar_statistics_cache"));
    }

    void InitializeReaderMemoryManager()
    {
        TParallelReaderMemoryManagerOptions parallelReaderMemoryManagerOptions{
            .TotalReservedMemorySize = Config_->TotalReaderMemoryLimit,
            .MaxInitialReaderReservedMemory = Config_->TotalReaderMemoryLimit,
            .EnableProfiling = true
        };
        ParallelReaderMemoryManager_ = CreateParallelReaderMemoryManager(
            parallelReaderMemoryManagerOptions,
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    void StartDiscovery()
    {
        NApi::TCreateNodeOptions createCliqueNodeOptions;
        createCliqueNodeOptions.IgnoreExisting = true;
        createCliqueNodeOptions.Recursive = true;
        createCliqueNodeOptions.Attributes = ConvertToAttributes(THashMap<TString, i64>{{"discovery_version", TDiscovery::Version}});
        WaitFor(RootClient_->CreateNode(
            Config_->Discovery->Directory,
            NObjectClient::EObjectType::MapNode,
            createCliqueNodeOptions))
            .ThrowOnError();

        Discovery_->StartPolling();

        auto attributes = ConvertToAttributes(THashMap<TString, INodePtr>{
            {"host", ConvertToNode(Config_->Address)},
            {"rpc_port", ConvertToNode(Ports_.Rpc)},
            {"monitoring_port", ConvertToNode(Ports_.Monitoring)},
            {"tcp_port", ConvertToNode(Ports_.Tcp)},
            {"http_port", ConvertToNode(Ports_.Http)},
            {"pid", ConvertToNode(getpid())},
            {"job_cookie", ConvertToNode(std::stoi(GetEnv("YT_JOB_COOKIE", /*default =*/ "0")))},
        });

        WaitFor(Discovery_->Enter(ToString(Config_->InstanceId), attributes))
            .ThrowOnError();

        // Update after entering the group guarantees that we will notify all
        // alive instances via gossip about new one.
        Discovery_->UpdateList();
    }

    void MakeGossip()
    {
        YT_LOG_DEBUG("Gossip started");

        auto nodes = Discovery_->List();
        std::vector<TFuture<NRpc::TTypedClientResponse<TRspProcessGossip>::TResult>> futures;
        futures.reserve(nodes.size());
        auto selfState = GetInstanceState();

        for (auto [_, attributes] : nodes) {
            auto channel = ChannelFactory_->CreateChannel(
                attributes->Get<TString>("host") + ":" + ToString(attributes->Get<ui64>("rpc_port")));
            TClickHouseServiceProxy proxy(channel);
            auto req = proxy.ProcessGossip();
            req->set_instance_id(ToString(Config_->InstanceId));
            req->set_instance_state(static_cast<int>(selfState));
            futures.push_back(req->Invoke());
        }
        auto responses = WaitFor(AllSet(futures))
            .ValueOrThrow();

        i64 bannedCount = 0;

        // TODO(max42): better logging.

        auto responseIt = responses.begin();
        for (auto [name, attributes] : nodes) {
            if (!responseIt->IsOK() || responseIt->Value()->instance_id() != name ||
                responseIt->Value()->instance_state() == EInstanceState::Stopped)
            {
                YT_LOG_WARNING("Banning instance (Address: %v, HttpPort: %v, TcpPort: %v, RpcPort: %v, JobId: %v, State: %v)",
                    attributes->Get<TString>("host"),
                    attributes->Get<ui64>("http_port"),
                    attributes->Get<ui64>("tcp_port"),
                    attributes->Get<ui64>("rpc_port"),
                    name,
                    (responseIt->IsOK() ? Format("%v", EInstanceState(responseIt->Value()->instance_state())) : "Request failed"));
                Discovery_->Ban(name);
                ++bannedCount;
            }
            ++responseIt;
        }

        YT_LOG_DEBUG("Gossip completed (Alive: %v, Banned: %v)", nodes.size() - bannedCount, bannedCount);
    }

    void DoHandleIncomingGossip(const TString& instanceId, EInstanceState state)
    {
        if (state != EInstanceState::Active) {
            YT_LOG_DEBUG("Banning instance (InstanceId: %v, State: %v)",
                instanceId,
                state);
            Discovery_->Ban(instanceId);
            return;
        }

        if (KnownInstances_.contains(instanceId)) {
            return;
        }

        auto& counter = UnknownInstancePingCounter_[instanceId];
        ++counter;

        YT_LOG_DEBUG("Received gossip from unknown instance (InstanceId: %v, State: %v, Counter: %v)",
            instanceId,
            state,
            counter);

        if (counter >= Config_->UnknownInstancePingLimit) {
            return;
        }

        for (const auto& [name, _] : Discovery_->List(/* eraseBanned */ false)) {
            if (KnownInstances_.insert(name).second) {
                UnknownInstancePingCounter_.erase(name);
            }
        }

        if (KnownInstances_.contains(instanceId))  {
            return;
        }

        Discovery_->UpdateList(Config_->UnknownInstanceAgeThreshold);
    }

    void CreateOrchidNode()
    {
        const auto& host = NNet::GetLocalHostName();
        NApi::TCreateNodeOptions options;
        options.Recursive = true;
        options.Force = true;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("remote_addresses", GetLocalAddresses({{"default", host}}, Ports_.Rpc));
        options.Attributes = std::move(attributes);

        const auto& jobCookie = GetEnv("YT_JOB_COOKIE");
        auto path = SysClickHouse + "/orchids/" + ToString(Config_->CliqueId) + "/" + jobCookie;

        WaitFor(RootClient_->CreateNode(path, EObjectType::Orchid, options))
            .ThrowOnError();

        YT_LOG_INFO("Initialized orchid node (Host: %v, Port: %v, OrchidNodePath: %v)",
            host,
            Ports_.Rpc,
            path);
    }

    void RegisterFactories()
    {
        RegisterFunctions();
        RegisterTableFunctions();
        RegisterConcatenatingTableFunctions();
        RegisterTableDictionarySource(Owner_);
        RegisterStorageDistributor();
        RegisterDataTypeBoolean();
    }
};

////////////////////////////////////////////////////////////////////////////////

THost::THost(
    IInvokerPtr controlInvoker,
    TPorts ports,
    TYtConfigPtr config,
    NApi::NNative::TConnectionConfigPtr connectionConfig)
    : Impl_(New<TImpl>(
        this,
        std::move(controlInvoker),
        std::move(config),
        std::move(connectionConfig),
        ports))
{ }

void THost::Start()
{
    Impl_->Start();
}

void THost::HandleIncomingGossip(const TString& instanceId, EInstanceState state)
{
    Impl_->HandleIncomingGossip(instanceId, state);
}

TFuture<void> THost::StopDiscovery()
{
    return Impl_->StopDiscovery();
}

void THost::ValidateReadPermissions(
    const std::vector<NYPath::TRichYPath>& paths,
    const TString& user)
{
    return Impl_->ValidateReadPermissions(paths, user);
}

std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> THost::GetObjectAttributes(
    const std::vector<NYPath::TYPath>& paths,
    const IClientPtr& client)
{
    return Impl_->GetObjectAttributes(paths, client);
}

const TObjectAttributeCachePtr&     THost::GetObjectAttributeCache() const
{
    return Impl_->GetObjectAttributeCache();
}

const IInvokerPtr& THost::GetControlInvoker() const
{
    return Impl_->GetControlInvoker();
}

const IInvokerPtr& THost::GetWorkerInvoker() const
{
    return Impl_->GetWorkerInvoker();
}

const IInvokerPtr& THost::GetClickHouseWorkerInvoker() const
{
    return Impl_->GetClickHouseWorkerInvoker();
}

const IInvokerPtr& THost::GetFetcherInvoker() const
{
    return Impl_->GetFetcherInvoker();
}

const IInvokerPtr& THost::GetClickHouseFetcherInvoker() const
{
    return Impl_->GetClickHouseFetcherInvoker();
}

TClusterNodes THost::GetNodes() const
{
    return Impl_->GetNodes();
}

void THost::HandleCrashSignal() const
{
    return Impl_->HandleCrashSignal();
}

void THost::HandleSigint()
{
    return Impl_->HandleSigint();
}

const IMultiReaderMemoryManagerPtr& THost::GetMultiReaderMemoryManager() const
{
    return Impl_->GetMultiReaderMemoryManager();
}

NApi::NNative::IClientPtr THost::GetRootClient() const
{
    return Impl_->GetRootClient();
}

NApi::NNative::IClientPtr THost::CreateClient(const TString& user)
{
    return Impl_->CreateClient(user);
}

TFuture<void> THost::GetIdleFuture() const
{
    return Impl_->GetIdleFuture();
}

TQueryRegistryPtr THost::GetQueryRegistry() const
{
    return Impl_->GetQueryRegistry();
}

void THost::SaveQueryRegistryState()
{
    Impl_->SaveQueryRegistryState();
}

TYtConfigPtr THost::GetConfig() const
{
    return Impl_->GetConfig();
}

EInstanceState THost::GetInstanceState() const
{
    return Impl_->GetInstanceState();
}

void THost::PopulateSystemDatabase(DB::IDatabase* systemDatabase) const
{
    return Impl_->PopulateSystemDatabase(systemDatabase);
}

std::shared_ptr<DB::IDatabase> THost::CreateYtDatabase() const
{
    return Impl_->CreateYtDatabase();
}

void THost::SetContext(DB::Context* context)
{
    Impl_->SetContext(context);
}

NTableClient::TTableColumnarStatisticsCachePtr THost::GetTableColumnarStatisticsCache() const
{
    return Impl_->GetTableColumnarStatisticsCache();
}

THost::~THost() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
