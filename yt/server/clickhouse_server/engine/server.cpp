#include "server.h"

#include "auth_token.h"
#include "cluster_tracker.h"
#include "config_manager.h"
#include "database.h"
#include "http_handler.h"
#include "logger.h"
#include "runtime_components_factory.h"
#include "system_tables.h"
#include "table_dictionary_source.h"
#include "table_functions.h"
#include "table_functions_concat.h"
#include "tcp_handler.h"

#include <yt/server/clickhouse_server/native/storage.h>

#include <yt/server/clickhouse_server/helpers/poco_config.h>

#include <yt/server/clickhouse_server/native/config.h>

//#include <AggregateFunctions/registerAggregateFunctions.h>
//#include <Common/Exception.h>
//#include <Common/StringUtils/StringUtils.h>
//#include <Common/config.h>
//#include <Common/getMultipleKeysFromConfig.h>
//#include <Common/getNumberOfPhysicalCPUCores.h>
//#include <Databases/DatabaseMemory.h>
//#include <Functions/registerFunctions.h>
//#include <IO/HTTPCommon.h>
//#include <Interpreters/AsynchronousMetrics.h>
//#include <Interpreters/Context.h>
//#include <Interpreters/ProcessList.h>
//#include <server/IServer.h>
//#include <Storages/System/attachSystemTables.h>
//#include <TableFunctions/registerTableFunctions.h>

//#include <common/DateLUT.h>
////#include <common/logger_useful.h>

//#include <Poco/DirectoryIterator.h>
//#include <Poco/Ext/LevelFilterChannel.h>
//#include <Poco/File.h>
//#include <Poco/Logger.h>
//#include <Poco/Net/HTTPServer.h>
//#include <Poco/Net/NetException.h>
//#include <Poco/Net/TCPServer.h>
//#include <Poco/String.h>
//#include <Poco/ThreadPool.h>
//#include <Poco/Util/LayeredConfiguration.h>
//#include <Poco/Util/XMLConfiguration.h>

#include <yt/core/ytree/fluent.h>

#include <util/system/hostname.h>

#include <atomic>
#include <memory>
#include <sstream>
#include <vector>

namespace DB {

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string GetCanonicalPath(std::string path)
{
    Poco::trimInPlace(path);
    if (path.empty()) {
        throw Exception("path configuration parameter is empty");
    }
    if (path.back() != '/') {
        path += '/';
    }
    return path;
}

////////////////////////////////////////////////////////////////////////////////

class TServer::TImpl
    : public IServer
{
private:
    const NNative::ILoggerPtr AppLogger;
    const NNative::IStoragePtr Storage;
    const NNative::ICoordinationServicePtr CoordinationService;
    const NNative::ICliqueAuthorizationManagerPtr CliqueAuthorizationManager_;
    const NNative::TConfigPtr NativeConfig_;
    const std::string CliqueId_;
    const std::string InstanceId_;
    ui16 TcpPort_;
    ui16 HttpPort_;

    NNative::IAuthorizationTokenPtr ServerAuthToken;

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
        NNative::ILoggerPtr logger,
        NNative::IStoragePtr storage,
        NNative::ICoordinationServicePtr coordinationService,
        NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager,
        NNative::TConfigPtr nativeConfig,
        std::string cliqueId,
        std::string instanceId,
        ui16 tcpPort,
        ui16 httpPort)
        : AppLogger(std::move(logger))
        , Storage(std::move(storage))
        , CoordinationService(std::move(coordinationService))
        , CliqueAuthorizationManager_(std::move(cliqueAuthorizationManager))
        , NativeConfig_(std::move(nativeConfig))
        , CliqueId_(std::move(cliqueId))
        , InstanceId_(std::move(instanceId))
        , TcpPort_(tcpPort)
        , HttpPort_(httpPort)
    {}

    void Start()
    {
        SetupRootLogger();
        CreateServerAuthToken();
        EngineConfig_ = new Poco::Util::LayeredConfiguration();
        EngineConfig_->add(ConvertToPocoConfig(ConvertToNode(NativeConfig_->Engine)));
        SetupLoggers();
        SetupExecutionClusterNodeTracker();
        SetupContext();
        WarmupDictionaries();
        EnterExecutionCluster();
        SetupHandlers();
    }

    void Shutdown()
    {
        ClusterNodeTicket->Release();
        ExecutionClusterNodeTracker->StopTrack();

        Poco::Logger* log = &logger();

        Cancelled = true;

        for (auto& server: Servers) {
            server->stop();
        }

        Servers.clear();
        ServerPool.reset();

        AsynchronousMetrics.reset();
        SessionCleaner.reset();

        // Ask to cancel background jobs all table engines, and also query_log.
        // It is important to do early, not in destructor of Context, because
        // table engines could use Context on destroy.
        CH_LOG_INFO(log, "Shutting down storages.");
        Context->shutdown();
        CH_LOG_DEBUG(log, "Shutted down storages.");

        // Explicitly destroy Context. It is more convenient than in destructor of Server,
        // because logger is still available.
        // At this moment, no one could own shared part of Context.
        Context.reset();
        CH_LOG_DEBUG(log, "Destroyed global context.");

        ExecutionClusterNodeTracker.reset();
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

private:
    void SetupRootLogger()
    {
        LogChannel = WrapToLogChannel(AppLogger);

        auto& rootLogger = Poco::Logger::root();
        rootLogger.close();
        rootLogger.setChannel(LogChannel);

        // default logging level during bootstrapping stage
        rootLogger.setLevel("information");
    }

    void CreateServerAuthToken()
    {
        auto serverUser = NativeConfig_->Engine->UserName;
        auto* auth = Storage->AuthTokenService();
        ServerAuthToken = CreateAuthToken(*auth, serverUser);
    }


    void SetupLoggers()
    {
        logger().setLevel(NativeConfig_->Engine->LogLevel);
    }

    void SetupExecutionClusterNodeTracker()
    {
        CH_LOG_INFO(&logger(), "Starting cluster node tracker...");

        ExecutionClusterNodeTracker = CreateClusterNodeTracker(
            CoordinationService,
            ServerAuthToken,
            NativeConfig_->Engine->CypressRootPath + "/cliques",
            TcpPort_);
    }

    void SetupContext()
    {
        Poco::Logger* log = &logger();

        auto storageHomePath = NativeConfig_->Engine->CypressRootPath;

        // Context contains all that query execution is dependent:
        // settings, available functions, data types, aggregate functions, databases...
        auto runtimeComponentsFactory = CreateRuntimeComponentsFactory(
            Storage,
            CliqueId_,
            ServerAuthToken,
            storageHomePath,
            CliqueAuthorizationManager_);
        Context = std::make_unique<DB::Context>(
            Context::createGlobal(std::move(runtimeComponentsFactory)));
        Context->setGlobalContext(*Context);
        Context->setApplicationType(Context::ApplicationType::SERVER);

        Context->setConfig(EngineConfig_);

        // TODO(max42): move into global config and make customizable.
        Context->setUsersConfig(ConvertToPocoConfig(NativeConfig_->Engine->ClickHouseUsers));

        registerFunctions();
        registerAggregateFunctions();
        registerTableFunctions();

        RegisterTableFunctionsExt(Storage);
        RegisterConcatenatingTableFunctions(Storage, ExecutionClusterNodeTracker);

        RegisterTableDictionarySource(Storage, ServerAuthToken);

        // Initialize DateLUT early, to not interfere with running time of first query.
        CH_LOG_DEBUG(log, "Initializing DateLUT.");
        DateLUT::instance();
        CH_LOG_TRACE(log, "Initialized DateLUT with time zone `" << DateLUT::instance().getTimeZone() << "'.");

        // Limit on total number of concurrently executed queries.
        Context->getProcessList().setMaxSize(EngineConfig_->getInt("max_concurrent_queries", 0));

        // Size of cache for uncompressed blocks. Zero means disabled.
        size_t uncompressedCacheSize = EngineConfig_->getUInt64("uncompressed_cache_size", 0);
        if (uncompressedCacheSize) {
            Context->setUncompressedCache(uncompressedCacheSize);
        }

        Context->setDefaultProfiles(*EngineConfig_);

        std::string path = GetCanonicalPath(NativeConfig_->Engine->DataPath);
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
                    CH_LOG_DEBUG(log, "Removing old temporary file " << it->path());
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
            auto defaultDatabase = CreateDatabase(Storage, ExecutionClusterNodeTracker);
            CH_LOG_INFO(log, "Main database is available under names 'default' and " << CliqueId_);
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
        Poco::Logger* log = &logger();

        const auto& settings = Context->getSettingsRef();

        ServerPool = std::make_unique<Poco::ThreadPool>(3, EngineConfig_->getInt("max_connections", 1024));

        auto listenHosts = NativeConfig_->Engine->ListenHosts;

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
                    CH_LOG_ERROR(log,
                        "Cannot resolve listen_host (" << host << "), error: " << e.message() << ". "
                        "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                        "specify IPv4 address to listen in <listen_host> element of configuration "
                        "file. Example: <listen_host>0.0.0.0</listen_host>");
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
                        CreateHttpHandlerFactory(*this),
                        *ServerPool,
                        socket,
                        httpParams));

                    CH_LOG_INFO(log, "Listening http://" + socketAddress.toString());
                }

                // TCP
                {
                    auto socketAddress = makeSocketAddress(listenHost, TcpPort_);

                    Poco::Net::ServerSocket socket(socketAddress);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);

                    Servers.emplace_back(new Poco::Net::TCPServer(
                        CreateTcpHandlerFactory(*this),
                        *ServerPool,
                        socket,
                        new Poco::Net::TCPServerParams()));

                    CH_LOG_INFO(log, "Listening tcp: " + socketAddress.toString());
                }
            } catch (const Poco::Net::NetException& e) {
                if (!(tryListen && e.code() == POCO_EPROTONOSUPPORT)) {
                    throw;
                }

                CH_LOG_ERROR(log, "Listen [" << listenHost << "]: " << e.what() << ": " << e.message()
                    << "  If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                    "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                    "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                    " Example for disabled IPv4: <listen_host>::</listen_host>");
            }
        }

        for (auto& server: Servers) {
            server->start();
        }

        CH_LOG_INFO(log, "Ready for connections.");
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

TServer::TServer(
    NNative::ILoggerPtr logger,
    NNative::IStoragePtr storage,
    NNative::ICoordinationServicePtr coordinationService,
    NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager,
    NNative::TConfigPtr nativeConfig,
    std::string cliqueId,
    std::string instanceId,
    ui16 tcpPort,
    ui16 httpPort)
    : Impl_(std::make_unique<TImpl>(
        std::move(logger),
        std::move(storage),
        std::move(coordinationService),
        std::move(cliqueAuthorizationManager),
        std::move(nativeConfig),
        std::move(cliqueId),
        std::move(instanceId),
        tcpPort,
        httpPort))
{ }

void TServer::Start()
{
    Impl_->Start();
}

void TServer::Shutdown()
{
    Impl_->Shutdown();
}

TServer::~TServer() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
