#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/uri/http_url.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/mem_limit.h>

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
    ui16 Port;

    void InitLogger() {
        NLog::ELevel level = NLog::ELevelHelpers::FromInt(Verbosity);
        NLog::EComponentHelpers::ForEach([level](NLog::EComponent c) {
            NYql::NLog::YqlLogger().SetComponentLevel(c, level);
        });
    }
};

void SignalHandler(int) {
    isInterrupted = 1;
}

int main(int argc, const char *argv[]) {
    try {
        SetInterruptSignalsHandler(SignalHandler);
        NYql::NLog::YqlLoggerScope logger(&Cerr);
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
        opts.AddLongOption('p', "port", "Worker server port").StoreResult(&options.Port).DefaultValue(7007);
        opts.SetFreeArgsMax(0);

        // TODO (@akozelskikh) - use port value for Http worker server

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        options.InitLogger();

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
            ythrow yexception() << "Invalid fast map reduce coordinator server url passed in parameters";
        }
        coordinatorClientSettings.Port = parsedUrl.GetPort();
        coordinatorClientSettings.Host = parsedUrl.GetHost();
        auto coordinator = MakeFmrCoordinatorClient(coordinatorClientSettings);

        auto fmrYtJobSerivce =  isNative ? MakeYtJobSerivce() : MakeFileYtJobSerivce();
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{
            .RunInSeparateProcess = true,
            .FmrJobBinaryPath = options.FmrJobBinaryPath
        });
        // TODO - add different job Settings here
        auto func = [options, fmrYtJobSerivce, jobLauncher] (NFmr::TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) mutable {
            return RunJob(task, options.TableDataServiceDiscoveryFilePath, fmrYtJobSerivce, jobLauncher , cancelFlag);
        };

        TFmrJobFactorySettings settings{.Function=func};
        auto jobFactory = MakeFmrJobFactory(settings);
        auto worker = MakeFmrWorker(coordinator, jobFactory, workerSettings);
        worker->Start();
        Cerr << "Fast map reduce worker has started\n";

        while (!isInterrupted) {
            Sleep(TDuration::Seconds(1));
        }
        worker->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
