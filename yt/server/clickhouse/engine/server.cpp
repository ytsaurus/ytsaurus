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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Databases/DatabaseMemory.h>
#include <Functions/registerFunctions.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <server/IServer.h>
#include <Storages/System/attachSystemTables.h>
#include <TableFunctions/registerTableFunctions.h>

#include <common/DateLUT.h>
#include <common/logger_useful.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Ext/LevelFilterChannel.h>
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

namespace DB {

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

}   // namespace DB

namespace NYT {
namespace NClickHouse {

using namespace DB;

namespace {

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
    , public NInterop::IServer
{
private:
    const NInterop::ILoggerPtr AppLogger;
    const NInterop::IStoragePtr Storage;
    const NInterop::ICoordinationServicePtr CoordinationService;
    const std::string ConfigFile;
    ui16 TcpPort_;
    ui16 HttpPort_;

    IConfigPtr StaticBootstrapConfig;

    NInterop::IAuthorizationTokenPtr ServerAuthToken;

    IConfigManagerPtr ConfigManager;

    Poco::AutoPtr<Poco::Util::LayeredConfiguration> Config;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> UsersConfig;
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> ClustersConfig;

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
    TServer(NInterop::ILoggerPtr logger,
            NInterop::IStoragePtr storage,
            NInterop::ICoordinationServicePtr coordinationService,
            std::string configFile,
            ui16 tcpPort,
            ui16 httpPort)
        : AppLogger(std::move(logger))
        , Storage(std::move(storage))
        , CoordinationService(std::move(coordinationService))
        , ConfigFile(std::move(configFile))
        , TcpPort_(tcpPort)
        , HttpPort_(httpPort)
    {}

    void Start() override;
    void Shutdown() override;

    Poco::Logger& logger() const override
    {
        return Poco::Logger::root();
    }

    Poco::Util::LayeredConfiguration& config() const override
    {
        return *const_cast<Poco::Util::LayeredConfiguration*>(Config.get());
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
    void SetupRootLogger();
    void LoadStaticBootstrapConfig();
    void CreateServerAuthToken();
    void SetupConfigurationManager();
    void SetupLoggers();
    void SetupExecutionClusterNodeTracker();
    void SetupContext();
    void WarmupDictionaries();
    void SetupHandlers();
    void EnterExecutionCluster();
};

////////////////////////////////////////////////////////////////////////////////

void TServer::Start()
{
    SetupRootLogger();
    LoadStaticBootstrapConfig();
    CreateServerAuthToken();
    SetupConfigurationManager();
    SetupLoggers();
    SetupExecutionClusterNodeTracker();
    SetupContext();
    WarmupDictionaries();
    EnterExecutionCluster();
    SetupHandlers();
}

void TServer::SetupRootLogger()
{
    LogChannel = WrapToLogChannel(AppLogger);

    auto& rootLogger = Poco::Logger::root();
    rootLogger.close();
    rootLogger.setChannel(LogChannel);

    // default logging level during bootstrapping stage
    rootLogger.setLevel("information");
}

void TServer::LoadStaticBootstrapConfig()
{
    StaticBootstrapConfig = LoadConfigFromLocalFile(ConfigFile);
}

void TServer::CreateServerAuthToken()
{
    auto serverUser = StaticBootstrapConfig->getString("auth.server_user");
    auto* auth = Storage->AuthTokenService();
    ServerAuthToken = CreateAuthToken(*auth, serverUser);
}

void TServer::SetupConfigurationManager()
{
    ConfigManager = CreateConfigManager(StaticBootstrapConfig, Storage, ServerAuthToken);

    Config = ConfigManager->LoadServerConfig();
    UsersConfig = ConfigManager->LoadUsersConfig();
    ClustersConfig = ConfigManager->LoadClustersConfig();
}

void TServer::SetupLoggers()
{
    logger().setLevel(Config->getString("logger.level", "trace"));

    Poco::Util::AbstractConfiguration::Keys levels;
    Config->keys("logger.levels", levels);

    for (auto it = levels.begin(); it != levels.end(); ++it) {
        Logger::get(*it).setLevel(Config->getString("logger.levels." + *it, "trace"));
    }
}

void TServer::SetupExecutionClusterNodeTracker()
{
    LOG_INFO(&logger(), "Starting cluster node tracker...");

    ExecutionClusterNodeTracker = CreateClusterNodeTracker(
        CoordinationService,
        ServerAuthToken,
        Config->getString("cluster_discovery.directory_path"),
        TcpPort_);
}

void TServer::SetupContext()
{
    Poco::Logger* log = &logger();

    auto storageHomePath = Config->getString("storage_home_path");

    // Context contains all that query execution is dependent:
    // settings, available functions, data types, aggregate functions, databases...
    auto runtimeComponentsFactory = CreateRuntimeComponentsFactory(Storage, ServerAuthToken, storageHomePath);
    Context = std::make_unique<DB::Context>(
        Context::createGlobal(std::move(runtimeComponentsFactory)));
    Context->setGlobalContext(*Context);
    Context->setApplicationType(Context::ApplicationType::SERVER);

    Context->setConfig(Config);
    Context->setUsersConfig(UsersConfig);
    Context->setClustersConfig(ClustersConfig);
    ConfigManager->SubscribeToUpdates(Context.get());

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();

    RegisterTableFunctionsExt(Storage);
    RegisterConcatenatingTableFunctions(Storage, ExecutionClusterNodeTracker);

    RegisterTableDictionarySource(Storage, ServerAuthToken);

    // Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone `" << DateLUT::instance().getTimeZone() << "'.");

    // Limit on total number of concurrently executed queries.
    Context->getProcessList().setMaxSize(Config->getInt("max_concurrent_queries", 0));

    // Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressedCacheSize = Config->getUInt64("uncompressed_cache_size", 0);
    if (uncompressedCacheSize) {
        Context->setUncompressedCache(uncompressedCacheSize);
    }

    // Load global settings from default profile.
    //std::string defaultProfileName = Config->getString("default_profile", "default");
    //Context->setDefaultProfileName(defaultProfileName);
    //Context->setSetting("profile", defaultProfileName);
    Context->setDefaultProfiles(*Config);

    std::string path = GetCanonicalPath(Config->getString("path"));
    Poco::File(path).createDirectories();
    Context->setPath(path);

    // Directory with temporary data for processing of hard queries.
    {
        std::string tmpPath = Config->getString("tmp_path", path + "tmp/");
        Poco::File(tmpPath).createDirectories();
        Context->setTemporaryPath(tmpPath);

        // Clearing old temporary files.
        for (Poco::DirectoryIterator it(tmpPath), end; it != end; ++it) {
            if (it->isFile() && startsWith(it.name(), "tmp")) {
                LOG_DEBUG(log, "Removing old temporary file " << it->path());
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
        Context->addDatabase("default", defaultDatabase);
    }

    std::string defaultDatabase = Config->getString("default_database", "default");
    Context->setCurrentDatabase(defaultDatabase);
}

void TServer::WarmupDictionaries()
{
    Context->getEmbeddedDictionaries();
    Context->getExternalDictionaries();
}

void TServer::SetupHandlers()
{
    Poco::Logger* log = &logger();

    const auto& settings = Context->getSettingsRef();

    ServerPool = std::make_unique<Poco::ThreadPool>(3, Config->getInt("max_connections", 1024));

    auto listenHosts = DB::getMultipleValuesFromConfig(*Config, "", "listen_host");

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
                LOG_ERROR(log,
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

                Poco::Timespan keepAliveTimeout(Config->getInt("keep_alive_timeout", 10), 0);

                Poco::Net::HTTPServerParams::Ptr httpParams = new Poco::Net::HTTPServerParams();
                httpParams->setTimeout(settings.receive_timeout);
                httpParams->setKeepAliveTimeout(keepAliveTimeout);

                Servers.emplace_back(new Poco::Net::HTTPServer(
                    CreateHttpHandlerFactory(*this),
                    *ServerPool,
                    socket,
                    httpParams));

                LOG_INFO(log, "Listening http://" + socketAddress.toString());
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

                LOG_INFO(log, "Listening tcp: " + socketAddress.toString());
            }
        } catch (const Poco::Net::NetException& e) {
            if (!(tryListen && e.code() == POCO_EPROTONOSUPPORT)) {
                throw;
            }

            LOG_ERROR(log, "Listen [" << listenHost << "]: " << e.what() << ": " << e.message()
                << "  If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>");
        }
    }

    for (auto& server: Servers) {
        server->start();
    }

    LOG_INFO(log, "Ready for connections.");
}

void TServer::EnterExecutionCluster()
{
    // TODO(max42): consider specifying two adresses, interconnect and external.
    ClusterNodeTicket = ExecutionClusterNodeTracker->EnterCluster(
        /*host=*/ Config->getString("interconnect_hostname", GetFQDNHostName()),
        /*port=*/ TcpPort_);

    ExecutionClusterNodeTracker->StartTrack(*Context);
}

void TServer::Shutdown()
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

    ConfigManager.reset();

    // Ask to cancel background jobs all table engines, and also query_log.
    // It is important to do early, not in destructor of Context, because
    // table engines could use Context on destroy.
    LOG_INFO(log, "Shutting down storages.");
    Context->shutdown();
    LOG_DEBUG(log, "Shutted down storages.");

    // Explicitly destroy Context. It is more convenient than in destructor of Server,
    // because logger is still available.
    // At this moment, no one could own shared part of Context.
    Context.reset();
    LOG_DEBUG(log, "Destroyed global context.");

    ExecutionClusterNodeTracker.reset();
}

////////////////////////////////////////////////////////////////////////////////

NInterop::IServerPtr CreateServer(
    NInterop::ILoggerPtr logger,
    NInterop::IStoragePtr storage,
    NInterop::ICoordinationServicePtr coordinationService,
    std::string configFile,
    ui16 tcpPort,
    ui16 httpPort)
{
    return std::make_shared<TServer>(
        std::move(logger),
        std::move(storage),
        std::move(coordinationService),
        std::move(configFile),
        tcpPort,
        httpPort);
}

}   // namespace NClickHouse
}   // namespace NYT
