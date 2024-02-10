#include "clickhouse_server.h"

#include "clickhouse_config.h"
#include "clickhouse_singletons.h"
#include "config_repository.h"
#include "config.h"
#include "format.h"
#include "helpers.h"
#include "host.h"
#include "http_handler.h"
#include "logger.h"
#include "poco_config.h"
#include "tcp_handler.h"
#include "user_defined_sql_objects_storage.h"

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/coroutine.h>

#include <Server/HTTP/HTTPServer.h>
#include <Server/TCPServer.h>
#include <Server/IServer.h>

#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <Common/ClickHouseRevision.h>
#include <Common/MemoryTracker.h>
#include <Common/SensitiveDataMasker.h>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/ServerAsynchronousMetrics.h>
#include <IO/SharedThreadPools.h>
#include <Storages/StorageMemory.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/System/StorageSystemMetrics.h>
#include <Storages/System/StorageSystemProcesses.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/ServerApplication.h>

#include <util/system/env.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

class TClickHouseServer
    : public DB::IServer
    , public IClickHouseServer
    , public NProfiling::ISensorProducer
{
public:
    TClickHouseServer(
        THost* host,
        TClickHouseConfigPtr config)
        : Host_(std::move(host))
        , Config_(config)
        , SharedContext_(DB::Context::createShared())
        , ServerContext_(DB::Context::createGlobal(SharedContext_.get()))
        , LayeredConfig_(ConvertToLayeredConfig(ConvertToNode(Config_)))
        , PocoApplication_(std::make_unique<Poco::Util::ServerApplication>())
    {
        // NOTE(dakovalkov): We do not use Poco's Application class directly, but it is used via
        // Application::instance() in Poco's SSLManager to obtain a config for initialization.
        // Although it's possible to initialize SSLManager manually without the Application instance,
        // it would require copy-pasting a lot of code from SSLManager::initDefaultContext.
        // So we just create "fake" Application, add the config to it and forget about it.
        PocoApplication_->config().add(LayeredConfig_);

        SetupLogger();

        // NB: under debug build this method does not fit in regular fiber stack
        // due to force inlining stack bloat: https://max42.at.yandex-team.ru/103.
        TCoroutine<void()> coroutine(BIND([&] (TCoroutine<void()>& /*self*/) {
            SetupContext();
        }), EExecutionStackKind::Large);
        coroutine.Run();
        YT_VERIFY(coroutine.IsCompleted());

        WarmupDictionaries();
    }

    // IClickHouseServer overrides:

    void Start() override
    {
        SetupServers();

        ClickHouseNativeProfiler.AddProducer("", MakeStrong(this));

        for (auto& server : Servers_) {
            server->start();
        }
    }

    void Stop() override
    {
        Cancelled_ = true;

        for (auto& server : Servers_) {
            if (auto httpPtr = dynamic_cast<DB::HTTPServer*>(server.get())) {
                // Special method of HTTP Server, will break all active connections.
                httpPtr->stopAll(true);
            } else {
                server->stop();
            }
        }
    }

    DB::ContextMutablePtr GetContext() override
    {
        return ServerContext_;
    }

    // DB::Server overrides:

    Poco::Logger& logger() const override
    {
        return Poco::Logger::root();
    }

    Poco::Util::LayeredConfiguration& config() const override
    {
        return *const_cast<Poco::Util::LayeredConfiguration*>(LayeredConfig_.get());
    }

    DB::ContextMutablePtr context() const override
    {
        return ServerContext_;
    }

    bool isCancelled() const override
    {
        return Cancelled_;
    }

private:
    THost* Host_;
    const TClickHouseConfigPtr Config_;
    DB::SharedContextHolder SharedContext_;
    DB::ContextMutablePtr ServerContext_;

    // Poco representation of Config_.
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> LayeredConfig_;

    // Fake Poco::Util::Application instance for proper SSLManager initialization.
    std::unique_ptr<Poco::Util::ServerApplication> PocoApplication_;

    Poco::AutoPtr<Poco::Channel> LogChannel;

    std::unique_ptr<DB::ServerAsynchronousMetrics> AsynchronousMetrics_;

    std::unique_ptr<Poco::ThreadPool> ServerPool_;
    std::vector<std::unique_ptr<DB::TCPServer>> Servers_;

    std::atomic<bool> Cancelled_ { false };

    std::shared_ptr<DB::IDatabase> SystemDatabase_;

    scope_guard DictionaryGuard_;

    void SetupLogger()
    {
        LogChannel = CreateLogChannel(ClickHouseNativeLogger);

        auto& rootLogger = Poco::Logger::root();
        rootLogger.close();
        rootLogger.setChannel(LogChannel);
        rootLogger.setLevel(Config_->LogLevel);
    }

    void SetupContext()
    {
        YT_LOG_INFO("Setting up context");

        GlobalThreadPool::initialize(
            Config_->MaxThreadPoolSize,
            Config_->MaxThreadPoolFreeSize,
            Config_->ThreadPoolQueueSize);

        DB::getIOThreadPool().initialize(
            Config_->MaxIOThreadPoolSize,
            Config_->MaxIOThreadPoolFreeSize,
            Config_->IOThreadPoolQueueSize);

        ServerContext_->makeGlobalContext();
        ServerContext_->setApplicationType(DB::Context::ApplicationType::SERVER);
        ServerContext_->setConfig(LayeredConfig_);
        ServerContext_->setUsersConfig(ConvertToPocoConfig(ConvertToNode(Config_->Users)));

        Host_->SetContext(ServerContext_);

        RegisterClickHouseSingletons();

        CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::getVersionRevision());
        CurrentMetrics::set(CurrentMetrics::VersionInteger, ClickHouseRevision::getVersionInteger());

        // Initialize DateLUT early, to not interfere with running time of first query.
        YT_LOG_DEBUG("Initializing DateLUT");
        DateLUT::setDefaultTimezone(Config_->Timezone.value());
        DateLUT::instance();
        YT_LOG_DEBUG("DateLUT initialized (TimeZone: %v)", DateLUT::instance().getTimeZone());

        // Limit on total number of concurrently executed queries.
        ServerContext_->getProcessList().setMaxSize(Config_->MaxConcurrentQueries);

        ServerContext_->setDefaultProfiles(*LayeredConfig_);

        YT_LOG_DEBUG("Profiles, processes & uncompressed cache set up");

        NFS::MakeDirRecursive(Config_->DataPath);
        ServerContext_->setPath(Config_->DataPath);

        // This function ss used only for thread_count metric per ProtocolServer.
        auto dummy_protocol_server_metric_func = [] () {
            return std::vector<DB::ProtocolServerMetrics>{};
        };

        // This object will periodically calculate asynchronous metrics.
        AsynchronousMetrics_ = std::make_unique<DB::ServerAsynchronousMetrics>(
            ServerContext_,
            /*update_period_seconds*/ 60,
            /*heavy_metrics_update_period_seconds*/ 120,
            dummy_protocol_server_metric_func);

        YT_LOG_DEBUG("Asynchronous metrics set up");

        // Database for system tables.

        YT_LOG_DEBUG("Setting up databases");

        SystemDatabase_ = std::make_shared<DB::DatabaseMemory>(DB::DatabaseCatalog::SYSTEM_DATABASE, ServerContext_);

        DB::DatabaseCatalog::instance().attachDatabase(DB::DatabaseCatalog::SYSTEM_DATABASE, SystemDatabase_);

        DB::attach<DB::StorageSystemProcesses>(ServerContext_, *SystemDatabase_, "processes");
        DB::attach<DB::StorageSystemMetrics>(ServerContext_, *SystemDatabase_, "metrics");
        DB::attach<DB::StorageSystemDictionaries>(ServerContext_, *SystemDatabase_, "dictionaries");
        DB::attachSystemTablesLocal(ServerContext_, *SystemDatabase_);
        DB::attachSystemTablesAsync(ServerContext_, *SystemDatabase_, *AsynchronousMetrics_);

        Host_->PopulateSystemDatabase(SystemDatabase_.get());

        DB::DatabaseCatalog::instance().attachDatabase("YT", Host_->CreateYtDatabase());
        ServerContext_->setCurrentDatabase("YT");

        auto DatabaseForTemporaryAndExternalTables = std::make_shared<DB::DatabaseMemory>(DB::DatabaseCatalog::TEMPORARY_DATABASE, ServerContext_);
        DB::DatabaseCatalog::instance().attachDatabase(DB::DatabaseCatalog::TEMPORARY_DATABASE, DatabaseForTemporaryAndExternalTables);

        YT_LOG_DEBUG("Initializing system logs");

        PrepareSystemLogTables();
        ServerContext_->initializeSystemLogs();

        YT_LOG_DEBUG("System logs initialized");

        if (Config_->MaxServerMemoryUsage) {
            total_memory_tracker.setOrRaiseHardLimit(*Config_->MaxServerMemoryUsage);
            total_memory_tracker.setDescription("(total)");
            total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);
        }

        YT_LOG_DEBUG("Setting up access manager");

        DB::AccessControl& accessControl = ServerContext_->getAccessControl();

        auto accessStorage = std::make_unique<DB::MemoryAccessStorage>(
            "access" /*storage_name*/,
            accessControl.getChangesNotifier(),
            false /*allow_backup*/);

        accessControl.addStorage(std::move(accessStorage));

        RegisterNewUser(accessControl, InternalRemoteUserName);

        YT_LOG_DEBUG("Adding external dictionaries from config");

        DictionaryGuard_ = ServerContext_->getExternalDictionariesLoader().addConfigRepository(CreateDictionaryConfigRepository(Config_->Dictionaries));

        YT_LOG_DEBUG("Setting chyt custom setting prefix");

        accessControl.setCustomSettingsPrefixes(std::vector<std::string>{"chyt_", "chyt."});

        YT_LOG_DEBUG("Disabling the use of the Amazon EC2 instance metadata service");

        // NB: ClickHouse uses patched version of aws-sdk-core library with custom EC2 metadata client.
        // See https://github.com/ClickHouse/aws-sdk-cpp/blob/6e9460f1b0a9af2aa7fd712fdefcc22e2aca6f66/src/aws-cpp-sdk-core/source/internal/AWSHttpResourceClient.cpp#L400.
        // Building CH with ordinary version of the aws library leads to nullptr dereference during EC2 metadata client usage (to get region, for example).
        // We disable the use of the aws ec2 metadata service with this env variable to avoid patching aws library.
        SetEnv("AWS_EC2_METADATA_DISABLED", "true");

        if (Config_->QueryMaskingRules) {
            YT_LOG_DEBUG("Setting up query masking rules");
            DB::SensitiveDataMasker::setInstance(std::make_unique<DB::SensitiveDataMasker>(*LayeredConfig_, "query_masking_rules"));
        }

        auto ytConfig = Host_->GetConfig();
        if (ytConfig->UserDefinedSqlObjectsStorage->Enabled) {
            YT_LOG_DEBUG("Setting up user defined SQL objects storage");
            ServerContext_->setUserDefinedSQLObjectsStorage(CreateUserDefinedSqlObjectsYTStorage(
                ServerContext_,
                ytConfig->UserDefinedSqlObjectsStorage,
                Host_));
            ServerContext_->getUserDefinedSQLObjectsStorage().loadObjects();
        }

        YT_LOG_DEBUG("Setting temporary storage");

        // NB: There is no way to disable disk usage for temporary data in ClickHouse,
        // so set a limit to 1 byte (0 means "unlimited").
        // ClickHouse doesn't always obey the limit, though:
        // https://github.com/ClickHouse/ClickHouse/issues/58826
        ServerContext_->setTemporaryStoragePath("tmp", /*max_temporary_data_on_disk_size*/ 1);

        YT_LOG_INFO("Finished setting up context");
    }

    void WarmupDictionaries()
    {
        YT_LOG_INFO("Warming up dictionaries");
        ServerContext_->getEmbeddedDictionaries();
        YT_LOG_INFO("Finished warming up");
    }

    void PrepareSystemLogTables()
    {
        auto queryLogConfig = Host_->GetConfig()->QueryLog;

        YT_LOG_DEBUG("Preparing tables for query log (AdditionalTableCount: %v)", queryLogConfig->AdditionalTables.size());

        auto queryLogTables = queryLogConfig->AdditionalTables;

        // In ClickHouse system.query_log is created after the first query has been executed.
        // It leads to an error if the first query is 'select * from system.query_log'.
        // To eliminate this, we explicitly create system.query_log among our own addition query log tables during startup.
        queryLogTables.push_back(TClickHouseTableConfig::Create("system", "query_log", Config_->QueryLog->Engine));

        for (const auto& table : queryLogTables) {
            YT_LOG_DEBUG("Preparing query log table (Database: %v, Name: %v, Engine: %v)",
                table->Database,
                table->Name,
                table->Engine);

            // NB: settings.query_settings contains bool/size_t fields with no default initialization.
            // {} here is to force a value (i.e. zero) initialization instead of a default initialization.
            DB::SystemLogSettings settings{};
            settings.engine = table->Engine;
            settings.queue_settings.database = table->Database;
            settings.queue_settings.table = table->Name;

            // NB: This is not a real QueryLog, it is needed only to create a table with proper query log structure.
            auto queryLog = std::make_shared<DB::QueryLog>(ServerContext_, settings);

            // prepareTable is public in ISystemLog interface, but is overwritten as private in QueryLog.
            auto* systemLog = static_cast<DB::ISystemLog*>(queryLog.get());
            systemLog->prepareTable();
            YT_LOG_DEBUG("Query log table prepared (Database: %v, Name: %v)",
                table->Database,
                table->Name);
        }

        YT_LOG_DEBUG("Tables for query log prepared");
    }

    void SetupServers()
    {
#ifdef _linux_
        YT_LOG_INFO("Setting up servers");

        const auto& settings = ServerContext_->getSettingsRef();

        ServerPool_ = std::make_unique<Poco::ThreadPool>(3, Config_->MaxConnections);

        auto setupSocket = [&] (UInt16 port) {
            Poco::Net::SocketAddress socketAddress;
            socketAddress = Poco::Net::SocketAddress(Poco::Net::SocketAddress::Family::IPv6, port);
            Poco::Net::ServerSocket socket(socketAddress);
            socket.setReceiveTimeout(settings.receive_timeout);
            socket.setSendTimeout(settings.send_timeout);

            return socket;
        };


        {
            YT_LOG_INFO("Setting up HTTP server");
            auto socket = setupSocket(Config_->HttpPort);

            Poco::Timespan keepAliveTimeout(Config_->KeepAliveTimeout, 0);

            Poco::Net::HTTPServerParams::Ptr httpParams = new Poco::Net::HTTPServerParams();
            httpParams->setTimeout(settings.receive_timeout);
            httpParams->setKeepAliveTimeout(keepAliveTimeout);

            Servers_.emplace_back(std::make_unique<DB::HTTPServer>(
                std::make_shared<DB::HTTPContext>(context()),
                CreateHttpHandlerFactory(Host_, *this),
                *ServerPool_,
                socket,
                httpParams));
        }

        {
            YT_LOG_INFO("Setting up TCP server");
            auto socket = setupSocket(Config_->TcpPort);

            Servers_.emplace_back(std::make_unique<DB::TCPServer>(
                CreateTcpHandlerFactory(Host_, *this),
                *ServerPool_,
                socket));
        }

        YT_LOG_INFO("Servers set up");
#endif
    }

    void CollectSensors(NProfiling::ISensorWriter* writer) override
    {
        for (int index = 0; index < static_cast<int>(CurrentMetrics::end()); ++index) {
            auto metric = CurrentMetrics::Metric(index);
            const auto* name = CurrentMetrics::getName(metric);
            auto value = CurrentMetrics::values[metric].load(std::memory_order::relaxed);

            writer->AddGauge("/current_metrics/" + CamelCaseToUnderscoreCase(TString(name)), value);
        }

        for (const auto& [name, metric] : AsynchronousMetrics_->getValues()) {
            writer->AddGauge("/asynchronous_metrics/" + CamelCaseToUnderscoreCase(TString(name)), metric.value);
        }

        for (int index = 0; index < static_cast<int>(ProfileEvents::end()); ++index) {
            auto event = ProfileEvents::Event(index);
            const auto* name = ProfileEvents::getName(event);
            auto value = ProfileEvents::global_counters[event].load(std::memory_order::relaxed);

            writer->AddCounter("/global_profile_events/" + CamelCaseToUnderscoreCase(TString(name)), value);
        }

        if (Config_->MaxServerMemoryUsage) {
            writer->AddGauge("/memory_limit", *Config_->MaxServerMemoryUsage);
        }

        for (
            auto tableIterator = SystemDatabase_->getTablesIterator(ServerContext_);
            tableIterator->isValid();
            tableIterator->next())
        {
            auto totalBytes = tableIterator->table()->totalBytes(ServerContext_->getSettingsRef());
            if (!totalBytes) {
                continue;
            }

            NProfiling::TWithTagGuard withTagGuard(writer, "table", TString(tableIterator->name()));
            writer->AddGauge("/system_tables/memory", *totalBytes);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IClickHouseServerPtr CreateClickHouseServer(
    THost* host,
    TClickHouseConfigPtr config)
{
    return New<TClickHouseServer>(std::move(host), std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseServer
