#include "host.h"

#include "clickhouse_service_proxy.h"
#include "config_repository.h"
#include "database.h"
#include "http_handler.h"
#include "logger.h"
#include "runtime_components_factory.h"
#include "system_tables.h"
#include "dictionary_source.h"
#include "table_functions.h"
#include "table_functions_concat.h"
#include "tcp_handler.h"
#include "private.h"
#include "query_context.h"
#include "query_registry.h"
#include "users_manager.h"
#include "poco_config.h"
#include "config.h"
#include "storage_distributor.h"

#include <yt/server/clickhouse_server/health_checker.h>

#include <yt/server/clickhouse_server/functions/public.h>
#include <yt/server/clickhouse_server/protos/clickhouse_service.pb.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/ytlib/security_client/permission_cache.h>

#include <yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/client/misc/discovery.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/crash_handler.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/caching_channel_factory.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/CurrentMetrics.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Databases/DatabaseMemory.h>
#include <Dictionaries/registerDictionaries.h>
#include <Functions/registerFunctions.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/IRuntimeComponentsFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <server/IServer.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>

#include <common/DateLUT.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/String.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <util/system/hostname.h>
#include <util/system/env.h>

#include <atomic>
#include <memory>
#include <sstream>
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

static const auto& Logger = ClickHouseYtLogger;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::string GetCanonicalPath(std::string path)
{
    Poco::trimInPlace(path);
    if (path.empty()) {
        throw DB::Exception("path configuration parameter is empty", DB::ErrorCodes::METRIKA_OTHER_ERROR);
    }
    if (path.back() != '/') {
        path += '/';
    }
    return path;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString> AttributesToCache{
    "id",
    "schema",
    "type",
    "dynamic",
    "chunk_count",
    "external",
    "external_cell_tag",
};

class TClickHouseHost::TImpl
    : public DB::IServer
    , public TRefCounted
{
private:
    TBootstrap* const Bootstrap_;
    const TClickHouseServerBootstrapConfigPtr Config_;
    const TString CliqueId_;
    const TString InstanceId_;
    const IInvokerPtr ControlInvoker_;
    ui16 RpcPort_;
    ui16 MonitoringPort_;
    ui16 TcpPort_;
    ui16 HttpPort_;
    TDiscoveryPtr Discovery_;

    Poco::AutoPtr<Poco::Util::LayeredConfiguration> EngineConfig_;

    Poco::AutoPtr<Poco::Channel> LogChannel;

    std::unique_ptr<DB::Context> DatabaseContext_;

    std::unique_ptr<DB::AsynchronousMetrics> AsynchronousMetrics_;
    std::unique_ptr<DB::SessionCleaner> SessionCleaner;

    std::unique_ptr<Poco::ThreadPool> ServerPool;
    std::vector<std::unique_ptr<Poco::Net::TCPServer>> Servers;

    std::atomic<bool> Cancelled { false };

    TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    TPeriodicExecutorPtr GossipExecutor_;

    NRpc::IChannelFactoryPtr ChannelFactory_;

    THashSet<TString> KnownInstances_;
    THashMap<TString, int> UnknownInstancePingCounter_;

    TPermissionCachePtr PermissionCache_;
    TObjectAttributeCachePtr TableAttributeCache_;

    THealthCheckerPtr HealthChecker_;

    IMultiReaderMemoryManagerPtr ParallelReaderMemoryManager_;

public:
    TImpl(
        TBootstrap* bootstrap,
        TClickHouseServerBootstrapConfigPtr config,
        std::string cliqueId,
        std::string instanceId,
        ui16 rpcPort,
        ui16 monitoringPort,
        ui16 tcpPort,
        ui16 httpPort)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , CliqueId_(std::move(cliqueId))
        , InstanceId_(std::move(instanceId))
        , ControlInvoker_(Bootstrap_->GetControlInvoker())
        , RpcPort_(rpcPort)
        , MonitoringPort_(monitoringPort)
        , TcpPort_(tcpPort)
        , HttpPort_(httpPort)
        , DatabaseContext_(std::make_unique<DB::Context>(
            DB::Context::createGlobal(CreateRuntimeComponentsFactory(
                CreateUsersManager(Bootstrap_, CliqueId_),
                CreateDictionaryConfigRepository(Config_->Engine->Dictionaries),
                std::make_unique<GeoDictionariesLoader>()))))
        // TODO(babenko): use permission cache from connection
        , PermissionCache_(New<TPermissionCache>(
            Config_->PermissionCache,
            Bootstrap_->GetConnection(),
            ClickHouseYtProfiler.AppendPath("/permission_cache")))
        , TableAttributeCache_(New<NObjectClient::TObjectAttributeCache>(
            Config_->TableAttributeCache,
            AttributesToCache,
            Bootstrap_->GetCacheClient(),
            Bootstrap_->GetControlInvoker(),
            Logger,
            ClickHouseYtProfiler.AppendPath("/object_attribute_cache")))
        , HealthChecker_(New<THealthChecker>(
            Config_->Engine->HealthChecker,
            Config_->User,
            DatabaseContext_.get(),
            Bootstrap_))
    {
        TParallelReaderMemoryManagerOptions parallelReaderMemoryManagerOptions(
            /* totalReservedMemorySize =*/Config_->TotalReaderMemoryLimit,
            /* maxInitialReaderReservedMemory =*/Config_->TotalReaderMemoryLimit,
            /* minRequiredMemorySize =*/0,
            /* profilingTagList =*/{},
            /* enableDetailedLogging =*/false,
            /* enableProfiling =*/true);
        // TODO(gritukan): Move it to NChunkClient::TDispatcher.
        ParallelReaderMemoryManager_ = CreateParallelReaderMemoryManager(
            parallelReaderMemoryManagerOptions,
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    void Start()
    {
        VERIFY_INVOKER_AFFINITY(GetControlInvoker());

        ChannelFactory_ = CreateCachingChannelFactory(CreateBusChannelFactory(New<NBus::TTcpBusConfig>()));

        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TImpl::CheckMemoryUsage, MakeWeak(this)),
            Config_->MemoryWatchdog->Period);
        MemoryWatchdogExecutor_->Start();

        SetupLogger();
        EngineConfig_ = new Poco::Util::LayeredConfiguration();
        EngineConfig_->add(ConvertToPocoConfig(ConvertToNode(Config_->Engine)));

        Discovery_ = New<TDiscovery>(
            Config_->Discovery,
            Bootstrap_->GetRootClient(),
            ControlInvoker_,
            std::vector<TString>{
                "host",
                "rpc_port",
                "monitoring_port",
                "tcp_port",
                "http_port",
                "pid",
                "job_cookie",
            },
            Logger);

        SetupContext();
        WarmupDictionaries();
        SetupHandlers();

        Discovery_->StartPolling();

        TAttributeMap attributes = {
            {"host", ConvertToNode(GetFQDNHostName())},
            {"rpc_port", ConvertToNode(RpcPort_)},
            {"monitoring_port", ConvertToNode(MonitoringPort_)},
            {"tcp_port", ConvertToNode(TcpPort_)},
            {"http_port", ConvertToNode(HttpPort_)},
            {"pid", ConvertToNode(getpid())},
            {"job_cookie", ConvertToNode(std::stoi(GetEnv("YT_JOB_COOKIE", /*default =*/ "0")))},
        };

        WaitFor(Discovery_->Enter(InstanceId_, attributes))
            .ThrowOnError();

        // Update after entering the group guarantees that we will notify all
        // alive instances via gossip about new one.
        Discovery_->UpdateList();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            Config_->ProfilingPeriod);
        ProfilingExecutor_->Start();

        GossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::MakeGossip, MakeWeak(this)),
            Config_->GossipPeriod);
        GossipExecutor_->Start();

        HealthChecker_->Start();
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

    void StopTcpServers()
    {
        for (auto& server : Servers) {
            if (auto httpPtr = dynamic_cast<Poco::Net::HTTPServer*>(server.get())) {
                // Special method of HTTP Server, will break all active connections.
                httpPtr->stopAll(true);
            } else {
                server->stop();
            }
        }
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
            THROW_ERROR_EXCEPTION("Error validating permissions for user %v", user) << errors;
        }
    }

    std::vector<TErrorOr<NYTree::TAttributeMap>> GetObjectAttributes(
        const std::vector<TYPath>& paths,
        const IClientPtr& client)
    {
        const auto& user = client->GetOptions().GetUser();
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

        auto attributesForMissedPaths = WaitFor(TableAttributeCache_->GetFromClient(missedPaths, client))
            .ValueOrThrow();

        std::vector<TErrorOr<NYTree::TAttributeMap>> attributes;
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


    Poco::Logger& logger() const override
    {
        return Poco::Logger::root();
    }

    Poco::Util::LayeredConfiguration& config() const override
    {
        return *const_cast<Poco::Util::LayeredConfiguration*>(EngineConfig_.get());
    }

    DB::Context& context() const override
    {
        return *DatabaseContext_;
    }

    bool isCancelled() const override
    {
        return Cancelled;
    }

    TClusterNodes GetNodes() const
    {
        auto nodeList = Discovery_->List();
        TClusterNodes result;
        result.reserve(nodeList.size());
        for (const auto& [_, attributes] : nodeList) {
            auto host = attributes.at("host")->AsString()->GetValue();
            auto tcpPort = attributes.at("tcp_port")->AsUint64()->GetValue();
            result.push_back(CreateClusterNode(TClusterNodeName{host, tcpPort}, DatabaseContext_->getSettingsRef(), TcpPort_));
        }
        return result;
    }

    void OnProfiling()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_DEBUG("Flushing profiling");

        Bootstrap_->GetQueryRegistry()->OnProfiling();

        for (int index = 0; index < static_cast<int>(CurrentMetrics::end()); ++index) {
            const auto* name = CurrentMetrics::getName(index);
            auto value = CurrentMetrics::values[index].load(std::memory_order_relaxed);
            ClickHouseNativeProfiler.Enqueue(
                "/current_metrics/" + CamelCaseToUnderscoreCase(TString(name)),
                value,
                EMetricType::Gauge);
        }

        for (const auto& [name, value] : AsynchronousMetrics_->getValues()) {
            ClickHouseNativeProfiler.Enqueue(
                "/asynchronous_metrics/" + CamelCaseToUnderscoreCase(TString(name)),
                value,
                EMetricType::Gauge);
        }

        for (int index = 0; index < static_cast<int>(ProfileEvents::end()); ++index) {
            const auto* name = ProfileEvents::getName(index);
            auto value = ProfileEvents::global_counters[index].load(std::memory_order_relaxed);
            ClickHouseNativeProfiler.Enqueue(
                "/global_profile_events/" + CamelCaseToUnderscoreCase(TString(name)),
                value,
                EMetricType::Counter);
        }

        if (Config_->CpuLimit) {
            ClickHouseYtProfiler.Enqueue(
                "/cpu_limit",
                *Config_->CpuLimit,
                EMetricType::Gauge);
        }

        HealthChecker_->OnProfiling();

        YT_LOG_DEBUG("Profiling flushed");
    }

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlInvoker_;
    }

    const IMultiReaderMemoryManagerPtr& GetMultiReaderMemoryManager() const
    {
        return ParallelReaderMemoryManager_;
    }

private:
    TPeriodicExecutorPtr ProfilingExecutor_;

    void SetupLogger()
    {
        LogChannel = CreateLogChannel(ClickHouseNativeLogger);

        auto& rootLogger = Poco::Logger::root();
        rootLogger.close();
        rootLogger.setChannel(LogChannel);
        rootLogger.setLevel(Config_->Engine->LogLevel);
    }

    void SetupContext()
    {
        YT_LOG_INFO("Setting up context");

        auto storageHomePath = Config_->Engine->CypressRootPath;

        DatabaseContext_->makeGlobalContext();
        DatabaseContext_->setApplicationType(DB::Context::ApplicationType::SERVER);

        DatabaseContext_->setConfig(EngineConfig_);

        DatabaseContext_->setUsersConfig(ConvertToPocoConfig(ConvertToNode(Config_->Engine->Users)));

        DB::registerFunctions();
        DB::registerAggregateFunctions();
        DB::registerTableFunctions();
        DB::registerStorageMemory(DB::StorageFactory::instance());
        DB::registerDictionaries();

        RegisterFunctions();
        RegisterTableFunctions();
        RegisterConcatenatingTableFunctions();
        RegisterTableDictionarySource(Bootstrap_);
        RegisterStorageDistributor();

        CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::get());
        CurrentMetrics::set(CurrentMetrics::VersionInteger, ClickHouseRevision::getVersionInteger());

        // Initialize DateLUT early, to not interfere with running time of first query.
        YT_LOG_INFO("Initializing DateLUT");
        DateLUT::instance();
        YT_LOG_INFO("DateLUT initialized (TimeZone: %v)", DateLUT::instance().getTimeZone());

        // Limit on total number of concurrently executed queries.
        DatabaseContext_->getProcessList().setMaxSize(EngineConfig_->getInt("max_concurrent_queries", 0));

        // Size of cache for uncompressed blocks. Zero means disabled.
        size_t uncompressedCacheSize = EngineConfig_->getUInt64("uncompressed_cache_size", 0);
        if (uncompressedCacheSize) {
            DatabaseContext_->setUncompressedCache(uncompressedCacheSize);
        }

        DatabaseContext_->setDefaultProfiles(*EngineConfig_);

        std::string path = GetCanonicalPath(Config_->Engine->DataPath);
        Poco::File(path).createDirectories();
        DatabaseContext_->setPath(path);

        // Directory with temporary data for processing of hard queries.
        {
            // TODO(max42): tmpfs here?
            std::string tmpPath = EngineConfig_->getString("tmp_path", path + "tmp/");
            Poco::File(tmpPath).createDirectories();
            DatabaseContext_->setTemporaryPath(tmpPath);

            // Clearing old temporary files.
            for (Poco::DirectoryIterator it(tmpPath), end; it != end; ++it) {
                if (it->isFile() && startsWith(it.name(), "tmp")) {
                    YT_LOG_DEBUG("Removing old temporary file (Path: %v)", it->path());
                    it->remove();
                }
            }
        }

        // This object will periodically calculate asynchronous metrics.
        AsynchronousMetrics_ = std::make_unique<DB::AsynchronousMetrics>(*DatabaseContext_);

        // This object will periodically cleanup sessions.
        SessionCleaner.reset(new DB::SessionCleaner(*DatabaseContext_));

        DatabaseContext_->initializeSystemLogs();

        // Database for system tables.
        {
            auto systemDatabase = std::make_shared<DB::DatabaseMemory>("system");

            AttachSystemTables(*systemDatabase, Discovery_, InstanceId_);

            if (AsynchronousMetrics_) {
                attachSystemTablesAsync(*systemDatabase, *AsynchronousMetrics_);
            }

            DatabaseContext_->addDatabase("system", systemDatabase);
        }

        // Default database that wraps connection to YT cluster.
        {
            auto defaultDatabase = CreateDatabase();
            DatabaseContext_->addDatabase("default", defaultDatabase);
            DatabaseContext_->addDatabase(CliqueId_, defaultDatabase);
        }

        std::string defaultDatabase = EngineConfig_->getString("default_database", "default");
        DatabaseContext_->setCurrentDatabase(defaultDatabase);
    }

    void WarmupDictionaries()
    {
        DatabaseContext_->getEmbeddedDictionaries();
        DatabaseContext_->getExternalDictionaries();
    }

    void SetupHandlers()
    {
        YT_LOG_INFO("Setting up handlers");

        const auto& settings = DatabaseContext_->getSettingsRef();

        ServerPool = std::make_unique<Poco::ThreadPool>(3, EngineConfig_->getInt("max_connections", 1024));

        auto listenHosts = Config_->Engine->ListenHosts;

        bool tryListen = false;
        if (listenHosts.empty()) {
            listenHosts.emplace_back("::1");
            listenHosts.emplace_back("127.0.0.1");
            tryListen = true;
        }

        auto makeSocketAddress = [&] (const std::string& host, UInt16 port) {
            Poco::Net::SocketAddress socketAddress;
            try {
                socketAddress = Poco::Net::SocketAddress(host, port);
            } catch (const Poco::Net::DNSException& e) {
                if (e.code() == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || e.code() == EAI_ADDRFAMILY
#endif
                    )
                {
                    YT_LOG_ERROR("Cannot resolve listen_host (Host: %v, Error: %v)", host, e.message());
                }

                throw;
            }
            return socketAddress;
        };

        for (const auto& listenHost: listenHosts) {
            try {
                // HTTP
                {
                    auto socketAddress = makeSocketAddress(listenHost, HttpPort_);

                    Poco::Net::ServerSocket socket(socketAddress);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);

                    Poco::Timespan keepAliveTimeout(EngineConfig_->getInt("keep_alive_timeout", 10), 0);

                    Poco::Net::HTTPServerParams::Ptr httpParams = new Poco::Net::HTTPServerParams();
                    httpParams->setTimeout(settings.receive_timeout);
                    httpParams->setKeepAliveTimeout(keepAliveTimeout);

                    Servers.emplace_back(new Poco::Net::HTTPServer(
                        CreateHttpHandlerFactory(Bootstrap_, *this),
                        *ServerPool,
                        socket,
                        httpParams));
                }

                // TCP
                {
                    auto socketAddress = makeSocketAddress(listenHost, TcpPort_);

                    Poco::Net::ServerSocket socket(socketAddress);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);

                    Servers.emplace_back(new Poco::Net::TCPServer(
                        CreateTcpHandlerFactory(Bootstrap_, *this),
                        *ServerPool,
                        socket,
                        new Poco::Net::TCPServerParams()));
                }
            } catch (const Poco::Net::NetException& e) {
                if (!(tryListen && e.code() == POCO_EPROTONOSUPPORT)) {
                    throw;
                }

                YT_LOG_ERROR("Error setting up listenHost (ListenHost: %v, What: %v, Error: %v)", listenHost, e.what(), e.message());
            }
        }

        for (auto& server: Servers) {
            server->start();
        }

        YT_LOG_INFO("Handlers set up");
    }

    void CheckMemoryUsage()
    {
        auto usage = GetProcessMemoryUsage();
        auto total = usage.Rss + usage.Shared;
        YT_LOG_DEBUG(
            "Checking memory usage "
            "(Rss: %v, Shared: %v, Total: %v, MemoryLimit: %v, CodicilWatermark: %v)",
            usage.Rss,
            usage.Shared,
            total,
            Config_->MemoryWatchdog->MemoryLimit,
            Config_->MemoryWatchdog->CodicilWatermark);
        if (total + Config_->MemoryWatchdog->CodicilWatermark > Config_->MemoryWatchdog->MemoryLimit) {
            YT_LOG_ERROR("We are close to OOM, printing query digest codicils and killing ourselves");
            NYT::NLogging::TLogManager::Get()->Shutdown();
            Bootstrap_->GetQueryRegistry()->WriteStateToStderr();
            Cerr << "*** RefCountedTracker ***\n" << Endl;
            Cerr << TRefCountedTracker::Get()->GetDebugInfo(2 /* sortByColumn */) << Endl;
            _exit(MemoryLimitExceededExitCode);
        }
    }

    void MakeGossip()
    {
        YT_LOG_DEBUG("Gossip started");
        auto nodes = Discovery_->List();
        std::vector<TFuture<NRpc::TTypedClientResponse<TRspProcessGossip>::TResult>> futures;
        futures.reserve(nodes.size());
        for (auto [_, attributes] : nodes) {
            auto channel = ChannelFactory_->CreateChannel(
                attributes["host"]->GetValue<TString>() + ":" + ToString(attributes["rpc_port"]->GetValue<ui64>()));
            TClickHouseServiceProxy proxy(channel);
            auto req = proxy.ProcessGossip();
            req->set_instance_id(InstanceId_);
            req->set_instance_state(static_cast<int>(Bootstrap_->GetState()));
            futures.push_back(req->Invoke());
        }
        auto responses = WaitFor(CombineAll(futures))
            .ValueOrThrow();

        i64 bannedCount = 0;

        auto responseIt = responses.begin();
        for (auto [name, attributes] : nodes) {
            if (!responseIt->IsOK() || responseIt->Value()->instance_id() != name ||
                responseIt->Value()->instance_state() == EInstanceState::Stopped)
            {
                YT_LOG_WARNING("Banning instance (Address: %v, HttpPort: %v, TcpPort: %v, RpcPort: %v, JobId: %v, State: %v)",
                    attributes["host"]->GetValue<TString>(),
                    attributes["http_port"]->GetValue<ui64>(),
                    attributes["tcp_port"]->GetValue<ui64>(),
                    attributes["rpc_port"]->GetValue<ui64>(),
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
};

////////////////////////////////////////////////////////////////////////////////

TClickHouseHost::TClickHouseHost(
    TBootstrap* bootstrap,
    TClickHouseServerBootstrapConfigPtr config,
    std::string cliqueId,
    std::string instanceId,
    ui16 rpcPort,
    ui16 monitoringPort,
    ui16 tcpPort,
    ui16 httpPort)
    : Impl_(New<TImpl>(
        bootstrap,
        std::move(config),
        std::move(cliqueId),
        std::move(instanceId),
        rpcPort,
        monitoringPort,
        tcpPort,
        httpPort))
{ }

void TClickHouseHost::Start()
{
    Impl_->Start();
}

void TClickHouseHost::HandleIncomingGossip(const TString& instanceId, EInstanceState state)
{
    Impl_->HandleIncomingGossip(instanceId, state);
}

TFuture<void> TClickHouseHost::StopDiscovery()
{
    return Impl_->StopDiscovery();
}

void TClickHouseHost::StopTcpServers()
{
    return Impl_->StopTcpServers();
}

void TClickHouseHost::ValidateReadPermissions(
    const std::vector<NYPath::TRichYPath>& paths,
    const TString& user)
{
    return Impl_->ValidateReadPermissions(paths, user);
}

std::vector<TErrorOr<NYTree::TAttributeMap>> TClickHouseHost::GetObjectAttributes(
    const std::vector<NYPath::TYPath>& paths,
    const IClientPtr& client)
{
    return Impl_->GetObjectAttributes(paths, client);
}

const IInvokerPtr& TClickHouseHost::GetControlInvoker() const
{
    return Impl_->GetControlInvoker();
}

DB::Context& TClickHouseHost::GetContext() const
{
    return Impl_->context();
}

TClusterNodes TClickHouseHost::GetNodes() const
{
    return Impl_->GetNodes();
}

const IMultiReaderMemoryManagerPtr& TClickHouseHost::GetMultiReaderMemoryManager() const
{
    return Impl_->GetMultiReaderMemoryManager();
}

TClickHouseHost::~TClickHouseHost() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
