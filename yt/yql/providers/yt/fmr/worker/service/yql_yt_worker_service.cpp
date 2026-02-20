#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/uri/http_url.h>
#include <util/string/strip.h>
#include <util/system/env.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_tvm_helpers.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/job_preparer/impl/yql_yt_job_preparer_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/worker/server/yql_yt_fmr_worker_server.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/mem_limit.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/essentials/protos/fmr.pb.h>

using namespace NYql::NFmr;
using namespace NYql;

volatile sig_atomic_t isInterrupted = 0;

struct TWorkerRunOptions {
    TString CoordinatorUrl;
    ui64 WorkerId;
    TString TableDataServiceDiscoveryFilePath;
    TString FmrJobBinaryPath;
    int Verbosity;
    TString UnderlyingGatewayType;
    TString Host;
    ui16 Port;
    TString LoggerFormat;
    THolder<TFileStorageConfig> FsConfig;
    THolder<TFmrFileRemoteCache> FmrRemoteCacheConfig;
    TString FmrTvmConfig;
    TMaybe<ui32> FmrTvmPort;
    TMaybe<TString> FmrTvmSecretPath;

    void InitLogger() {
        NLog::ELevel level = NLog::TLevelHelpers::FromInt(Verbosity);
        NLog::TComponentHelpers::ForEach([level](NLog::EComponent c) {
            NYql::NLog::YqlLogger().SetComponentLevel(c, level);
        });
    }
};

void SignalHandler(int) {
    isInterrupted = 1;
}

void LoadFmrRemoteCacheConfigFromFile(TStringBuf path, TFmrFileRemoteCache& params) {
    auto fs = TFsPath(path);
    try {
        ParseFromTextFormat(fs, params);
    } catch (...) {
        ythrow yexception() << "Bad format of fmr remote cache config settings: " << CurrentExceptionMessage();
    }
}


int main(int argc, const char *argv[]) {
    try {
        SetInterruptSignalsHandler(SignalHandler);
        TWorkerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption("coordinator-url", "Fast map reduce coordinator server url").Required().StoreResult(&options.CoordinatorUrl);
        opts.AddLongOption('w', "worker-id", "Fast map reduce worker id").Required().StoreResult(&options.WorkerId);
        opts.AddLongOption('v', "verbosity", "Logging verbosity level").StoreResult(&options.Verbosity).DefaultValue(static_cast<int>(TLOG_ERR));
        opts.AddLongOption('b', "fmrjob-binary-path", "Path to fmrjob map binary").StoreResult(&options.FmrJobBinaryPath);
        opts.AddLongOption('d', "table-data-service-discovery-file-path", "Table data service discovery file path").StoreResult(&options.TableDataServiceDiscoveryFilePath);
        opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, SetAddressSpaceLimit);
        opts.AddLongOption('g', "gateway-type", "Type of underlying gateway (native, file)").StoreResult(&options.UnderlyingGatewayType).DefaultValue("native");
        opts.AddLongOption('h', "host", "Fast map reduce worker server host").StoreResult(&options.Host).DefaultValue("localhost");
        opts.AddLongOption('p', "port", "Worker server port").StoreResult(&options.Port).DefaultValue(7007);
        opts.AddLongOption('f', "logger-format", "Logs formatting type").StoreResult(&options.LoggerFormat).DefaultValue("legacy");
        opts.AddLongOption("fs-cfg", "Fs configuration file").Optional().RequiredArgument("FILE").Handler1T<TString>([&options](const TString& file) {
            options.FsConfig = MakeHolder<TFileStorageConfig>();
            LoadFsConfigFromFile(file, *options.FsConfig);
        });
        opts.AddLongOption("fmr-cache-cfg", "Fmr remote cache configuration file").Optional().RequiredArgument("FILE").Handler1T<TString>([&options](const TString& file) {
            options.FmrRemoteCacheConfig = MakeHolder<TFmrFileRemoteCache>();
            LoadFmrRemoteCacheConfigFromFile(file, *options.FmrRemoteCacheConfig);
        });
        opts.AddLongOption('t', "tvm-cfg", "fmr tvm config").Optional().StoreResult(&options.FmrTvmConfig);
        opts.AddLongOption("tvm-port", "fmr tvm port").Optional().StoreResult(&options.FmrTvmPort);
        opts.AddLongOption("tvm-secret-path", "fmr tvm secret path").Optional().StoreResult(&options.FmrTvmSecretPath);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        TString loggerFormat = options.LoggerFormat;
        YQL_ENSURE(loggerFormat == "json" || loggerFormat == "legacy");
        auto formatter = loggerFormat == "json" ? NYql::NLog::JsonFormat : NYql::NLog::LegacyFormat;
        NYql::NLog::YqlLoggerScope logger(&Cerr, formatter);

        options.InitLogger();

        if (!options.FsConfig) {
            options.FsConfig = MakeHolder<TFileStorageConfig>();
            YQL_ENSURE(NResource::Has("fs.conf"));
            LoadFsConfigFromResource("fs.conf", *options.FsConfig);
        }

        TString underlyingGatewayType = options.UnderlyingGatewayType;
        if (underlyingGatewayType != "native" && underlyingGatewayType != "file") {
            throw yexception() << " Incorrect gateway type " << underlyingGatewayType << " passed in parameters";
        }
        bool isNative = underlyingGatewayType == "native";

        TFmrWorkerSettings workerSettings{};
        workerSettings.WorkerId = options.WorkerId;

        TFmrCoordinatorClientSettings coordinatorClientSettings;
        THttpURL parsedUrl;
        if (parsedUrl.Parse(options.CoordinatorUrl) != THttpURL::ParsedOK) {
            ythrow yexception() << "Invalid fast map reduce coordinator server url passed in parameters " << options.CoordinatorUrl;
        }
        coordinatorClientSettings.Port = parsedUrl.GetPort();
        coordinatorClientSettings.Host = parsedUrl.GetHost();

        IFmrTvmClient::TPtr tvmClient = nullptr;

        TMaybe<TFmrTvmJobSettings> tvmSettings = Nothing();
        TTvmId tableDataServiceTvmId = 0;
        auto tvmSpec = ParseFmrTvmSpec(options.FmrTvmConfig);

        if (tvmSpec.Defined()) {
            auto tvmSecret = ParseFmrTvmSecretFile(options.FmrTvmSecretPath);
            coordinatorClientSettings.DestinationTvmId = tvmSpec->CoordinatorTvmId;
            tvmClient = MakeFmrTvmClient(TFmrTvmToolSettings{
                .SourceTvmAlias = tvmSpec->WorkerTvmAlias,
                .TvmPort = options.FmrTvmPort,
                .TvmSecret = tvmSecret
            });
            tvmSettings = TFmrTvmJobSettings{
                .WorkerTvmAlias = tvmSpec->WorkerTvmAlias,
                .TableDataServiceTvmId = tvmSpec->TableDataServiceTvmId,
                .TvmPort = options.FmrTvmPort,
                .TvmSecret = tvmSecret
            };
            tableDataServiceTvmId = tvmSpec->TableDataServiceTvmId;
        }
        auto coordinator = MakeFmrCoordinatorClient(coordinatorClientSettings, tvmClient);

        auto fmrYtJobSerivce =  isNative ? MakeYtJobSerivce() : MakeFileYtJobService();
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{
            .RunInSeparateProcess = true,
            .FmrJobBinaryPath = options.FmrJobBinaryPath,
            .TableDataServiceDiscoveryFilePath = options.TableDataServiceDiscoveryFilePath,
            .GatewayType = underlyingGatewayType
        });
        // TODO - add different job Settings here
        TString tableDataServiceDiscoveryFilePath = options.TableDataServiceDiscoveryFilePath;
        auto func = [tableDataServiceDiscoveryFilePath, fmrYtJobSerivce, jobLauncher, tvmSettings] (NFmr::TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) mutable {
            return RunJob(task, tableDataServiceDiscoveryFilePath, fmrYtJobSerivce, jobLauncher, cancelFlag, tvmSettings);
        };

        TFmrJobFactorySettings settings{.Function=func};
        auto jobFactory = MakeFmrJobFactory(settings);
        auto&& fmrCacheConfig = options.FmrRemoteCacheConfig;

        TString ytDownloaderServer;
        if (fmrCacheConfig) {
            ytDownloaderServer = fmrCacheConfig->GetCluster();
        }
        NYql::NFS::IDownloaderPtr ytDownloader =  MakeYtDownloader(*options.FsConfig, ytDownloaderServer);
        TFileStoragePtr fileStorage = WithAsync(CreateFileStorage(*options.FsConfig, {ytDownloader}));

        auto jobPreparer = MakeFmrJobPreparer(fileStorage, tableDataServiceDiscoveryFilePath, TFmrJobPreparerSettings(), tvmClient, tableDataServiceTvmId);
        if (isNative && fmrCacheConfig && !fmrCacheConfig->GetPath().empty()) {
            TString distFileCacheBaseUrl = "yt://" + fmrCacheConfig->GetCluster() + "/" + fmrCacheConfig->GetPath();
            TString distCacheYtToken;
            if (!fmrCacheConfig->GetTokenFile().empty()) {
                TString tokenFile = fmrCacheConfig->GetTokenFile();
                YQL_ENSURE(NFs::Exists(tokenFile), "Token file should exist, if it is passed from fmr cache config");
                distCacheYtToken = StripStringRight(TFileInput(tokenFile).ReadLine());
            }
            jobPreparer->InitalizeDistributedCache(distFileCacheBaseUrl, distCacheYtToken);
        }

        auto worker = MakeFmrWorker(coordinator, jobFactory, jobPreparer, workerSettings);
        worker->Start();
        TFmrWorkerServerSettings workerServerSettings{.Port=options.Port, .Host = options.Host};
        auto workerServer = MakeFmrWorkerServer(workerServerSettings, worker);
        YQL_CLOG(TRACE, FastMapReduce) << "Fast map reduce worker has started";
        workerServer->Start();
        while (!isInterrupted) {
            Sleep(TDuration::Seconds(1));
        }
        worker->Stop();
        workerServer->Stop();
        YQL_CLOG(TRACE, FastMapReduce) << "Fast map reduce worker has stopped";
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
