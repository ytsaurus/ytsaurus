#include "host.h"

#include "config_repository.h"
#include "cluster_tracker.h"
#include "database.h"
#include "functions.h"
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
#include "security_manager.h"
#include "poco_config.h"
#include "config.h"
#include "storage_table.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/profiling/profile_manager.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
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
#include <server/IServer.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>

#include <common/DateLUT.h>
#include <common/logger_useful.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/String.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <util/system/hostname.h>

#include <atomic>
#include <memory>
#include <sstream>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NProfiling;

const auto& Logger = ServerLogger;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::string GetCanonicalPath(std::string path)
{
    Poco::trimInPlace(path);
    if (path.empty()) {
        throw Exception("path configuration parameter is empty", DB::ErrorCodes::METRIKA_OTHER_ERROR);
    }
    if (path.back() != '/') {
        path += '/';
    }
    return path;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TClickHouseHost::TImpl
    : public IServer
    , public TRefCounted
{
private:
    TBootstrap* const Bootstrap_;
    const ICoordinationServicePtr CoordinationService;
    const TClickHouseServerBootstrapConfigPtr Config_;
    const TString CliqueId_;
    const TString InstanceId_;
    const IInvokerPtr ControlInvoker_;
    ui16 TcpPort_;
    ui16 HttpPort_;

    Poco::AutoPtr<Poco::Util::LayeredConfiguration> EngineConfig_;

    Poco::AutoPtr<Poco::Channel> LogChannel;

    std::unique_ptr<DB::Context> Context;

    std::unique_ptr<DB::AsynchronousMetrics> AsynchronousMetrics;
    std::unique_ptr<DB::SessionCleaner> SessionCleaner;

    std::unique_ptr<Poco::ThreadPool> ServerPool;
    std::vector<std::unique_ptr<Poco::Net::TCPServer>> Servers;

    IClusterNodeTrackerPtr ExecutionClusterNodeTracker;
    TClusterNodeTicket ClusterNodeTicket;

    std::atomic<bool> Cancelled { false };

public:
    TImpl(
        TBootstrap* bootstrap,
        ICoordinationServicePtr coordinationService,
        TClickHouseServerBootstrapConfigPtr config,
        std::string cliqueId,
        std::string instanceId,
        ui16 tcpPort,
        ui16 httpPort)
        : Bootstrap_(bootstrap)
        , CoordinationService(std::move(coordinationService))
        , Config_(std::move(config))
        , CliqueId_(std::move(cliqueId))
        , InstanceId_(std::move(instanceId))
        , ControlInvoker_(Bootstrap_->GetControlInvoker())
        , TcpPort_(tcpPort)
        , HttpPort_(httpPort)
    {}

    void Start()
    {
        VERIFY_INVOKER_AFFINITY(GetControlInvoker());

        SetupLogger();
        EngineConfig_ = new Poco::Util::LayeredConfiguration();
        EngineConfig_->add(ConvertToPocoConfig(ConvertToNode(Config_->Engine)));
        SetupExecutionClusterNodeTracker();
        SetupContext();
        WarmupDictionaries();
        EnterExecutionCluster();
        SetupHandlers();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            Config_->ProfilingPeriod);
        ProfilingExecutor_->Start();
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
        return *Context;
    }

    bool isCancelled() const override
    {
        return Cancelled;
    }

    // TODO(max42): refactor.
    IClusterNodeTrackerPtr GetExecutionClusterNodeTracker() const
    {
        return ExecutionClusterNodeTracker;
    }

    void OnProfiling()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_DEBUG("Flushing profiling");

        for (auto& [user, runningQueryCount] : UserToRunningInitialQueryCount_) {
            ServerProfiler.Enqueue(
                "/running_initial_query_count",
                runningQueryCount,
                EMetricType::Gauge,
                {TProfileManager::Get()->RegisterTag("user", user)});
        }

        for (auto& [user, runningQueryCount] : UserToRunningSecondaryQueryCount_) {
            ServerProfiler.Enqueue(
                "/running_secondary_query_count",
                runningQueryCount,
                EMetricType::Gauge,
                {TProfileManager::Get()->RegisterTag("user", user)});
        }
    }

    void AdjustQueryCount(const TString& user, EQueryKind queryKind, int delta)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto& queryCountMap = (queryKind == EQueryKind::InitialQuery)
            ? UserToRunningInitialQueryCount_
            : UserToRunningSecondaryQueryCount_;
        THashMap<TString, int>::insert_ctx ctx;
        auto it = queryCountMap.find(user, ctx);
        if (it == queryCountMap.end()) {
            it = queryCountMap.emplace_direct(ctx, user, delta);
        } else {
            it->second += delta;
        }
        YCHECK(it->second >= 0);
        if (it->second == 0) {
            queryCountMap.erase(it);
        }
    }

    const IInvokerPtr& GetControlInvoker() const
    {
        return ControlInvoker_;
    }

private:
    TPeriodicExecutorPtr ProfilingExecutor_;
    THashMap<TString, int> UserToRunningInitialQueryCount_;
    THashMap<TString, int> UserToRunningSecondaryQueryCount_;

    void SetupLogger()
    {
        LogChannel = CreateLogChannel(EngineLogger);

        auto& rootLogger = Poco::Logger::root();
        rootLogger.close();
        rootLogger.setChannel(LogChannel);
        rootLogger.setLevel(Config_->Engine->LogLevel);
    }

    void SetupExecutionClusterNodeTracker()
    {
        YT_LOG_INFO("Starting cluster node tracker");

        ExecutionClusterNodeTracker = CreateClusterNodeTracker(
            CoordinationService,
            Config_->Engine->CypressRootPath + "/cliques",
            TcpPort_);
    }

    void SetupContext()
    {
        YT_LOG_INFO("Setting up context");

        auto storageHomePath = Config_->Engine->CypressRootPath;

        auto securityManager = CreateUsersManager(Bootstrap_, CliqueId_);
        auto dictionariesConfigRepository = CreateDictionaryConfigRepository(Config_->Engine->Dictionaries);
        auto geoDictionariesLoader = std::make_unique<GeoDictionariesLoader>();
        auto runtimeComponentsFactory = CreateRuntimeComponentsFactory(
            std::move(securityManager),
            std::move(dictionariesConfigRepository),
            std::move(geoDictionariesLoader));

        Context = std::make_unique<DB::Context>(Context::createGlobal(std::move(runtimeComponentsFactory)));
        Context->setGlobalContext(*Context);
        Context->setApplicationType(Context::ApplicationType::SERVER);

        Context->setConfig(EngineConfig_);

        Context->setUsersConfig(ConvertToPocoConfig(ConvertToNode(Config_->Engine->Users)));

        registerFunctions();
        registerAggregateFunctions();
        registerTableFunctions();
        registerStorageMemory(StorageFactory::instance());
        registerDictionaries();

        RegisterFunctions();
        RegisterTableFunctions();
        RegisterConcatenatingTableFunctions();
        RegisterTableDictionarySource(Bootstrap_);
        RegisterStorageTable();

        // Initialize DateLUT early, to not interfere with running time of first query.
        YT_LOG_INFO("Initializing DateLUT");
        DateLUT::instance();
        YT_LOG_INFO("DateLUT initialized (TimeZone: %v)", DateLUT::instance().getTimeZone());

        // Limit on total number of concurrently executed queries.
        Context->getProcessList().setMaxSize(EngineConfig_->getInt("max_concurrent_queries", 0));

        // Size of cache for uncompressed blocks. Zero means disabled.
        size_t uncompressedCacheSize = EngineConfig_->getUInt64("uncompressed_cache_size", 0);
        if (uncompressedCacheSize) {
            Context->setUncompressedCache(uncompressedCacheSize);
        }

        Context->setDefaultProfiles(*EngineConfig_);

        std::string path = GetCanonicalPath(Config_->Engine->DataPath);
        Poco::File(path).createDirectories();
        Context->setPath(path);

        // Directory with temporary data for processing of hard queries.
        {
            // TODO(max42): tmpfs here?
            std::string tmpPath = EngineConfig_->getString("tmp_path", path + "tmp/");
            Poco::File(tmpPath).createDirectories();
            Context->setTemporaryPath(tmpPath);

            // Clearing old temporary files.
            for (Poco::DirectoryIterator it(tmpPath), end; it != end; ++it) {
                if (it->isFile() && startsWith(it.name(), "tmp")) {
                    YT_LOG_DEBUG("Removing old temporary file (Path: %v)", it->path());
                    it->remove();
                }
            }
        }

#if defined(COLLECT_ASYNCHRONUS_METRICS)
        // This object will periodically calculate some metrics.
        AsynchronousMetrics.reset(new DB::AsynchronousMetrics(*Context));
#endif

        // This object will periodically cleanup sessions.
        SessionCleaner.reset(new DB::SessionCleaner(*Context));

        Context->initializeSystemLogs();

        // Database for system tables.
        {
            auto systemDatabase = std::make_shared<DatabaseMemory>("system");

            AttachSystemTables(*systemDatabase, ExecutionClusterNodeTracker);

            if (AsynchronousMetrics) {
                attachSystemTablesAsync(*systemDatabase, *AsynchronousMetrics);
            }

            Context->addDatabase("system", systemDatabase);
        }

        // Default database that wraps connection to YT cluster.
        {
            auto defaultDatabase = CreateDatabase(ExecutionClusterNodeTracker);
            Context->addDatabase("default", defaultDatabase);
            Context->addDatabase(CliqueId_, defaultDatabase);
        }

        std::string defaultDatabase = EngineConfig_->getString("default_database", "default");
        Context->setCurrentDatabase(defaultDatabase);
    }

    void WarmupDictionaries()
    {
        Context->getEmbeddedDictionaries();
        Context->getExternalDictionaries();
    }

    void SetupHandlers()
    {
        YT_LOG_INFO("Setting up handlers");

        const auto& settings = Context->getSettingsRef();

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

    void EnterExecutionCluster()
    {
        ClusterNodeTicket = ExecutionClusterNodeTracker->EnterCluster(
            InstanceId_,
            EngineConfig_->getString("interconnect_hostname", GetFQDNHostName()),
            TcpPort_,
            HttpPort_);

        ExecutionClusterNodeTracker->StartTrack(*Context);
    }
};

////////////////////////////////////////////////////////////////////////////////

TClickHouseHost::TClickHouseHost(
    TBootstrap* bootstrap,
    ICoordinationServicePtr coordinationService,
    TClickHouseServerBootstrapConfigPtr config,
    std::string cliqueId,
    std::string instanceId,
    ui16 tcpPort,
    ui16 httpPort)
    : Impl_(New<TImpl>(
        bootstrap,
        std::move(coordinationService),
        std::move(config),
        std::move(cliqueId),
        std::move(instanceId),
        tcpPort,
        httpPort))
{ }

void TClickHouseHost::Start()
{
    Impl_->Start();
}

void TClickHouseHost::AdjustQueryCount(const TString& user, EQueryKind queryKind, int delta)
{
    Impl_->AdjustQueryCount(user, queryKind, delta);
}

const IInvokerPtr& TClickHouseHost::GetControlInvoker() const
{
    return Impl_->GetControlInvoker();
}

IClusterNodeTrackerPtr TClickHouseHost::GetExecutionClusterNodeTracker() const
{
    return Impl_->GetExecutionClusterNodeTracker();
}

TClickHouseHost::~TClickHouseHost() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
