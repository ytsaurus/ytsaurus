#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/uri/http_url.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/yql_yt_table_data_service_client.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/yt_service/impl/yql_yt_yt_service_impl.h>
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
    int Verbosity;

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
        opts.AddLongOption('p', "table-data-service-discovery-file-path", "Table data service discovery file path").StoreResult(&options.TableDataServiceDiscoveryFilePath);
        opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, SetAddressSpaceLimit);
        opts.SetFreeArgsMax(0);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        options.InitLogger();

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

        ITableDataService::TPtr tableDataService;
        if (options.TableDataServiceDiscoveryFilePath) {
            auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = options.TableDataServiceDiscoveryFilePath});
            tableDataService = MakeTableDataServiceClient(tableDataServiceDiscovery);
        } else {
            tableDataService = MakeLocalTableDataService(TLocalTableDataServiceSettings(3));
        }
        auto fmrYtSerivce = MakeFmrYtSerivce();
        TFmrJobSettings jobSettings{};
        // TODO - add different job Settings here
        auto func = [tableDataService, fmrYtSerivce, jobSettings] (TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) mutable {
            return RunJob(task, tableDataService, fmrYtSerivce, cancelFlag, jobSettings);
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
