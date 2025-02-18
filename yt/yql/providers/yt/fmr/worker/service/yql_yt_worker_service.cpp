#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/uri/http_url.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>


using namespace NYql::NFmr;

bool isInterrupted = false;

struct TWorkerRunOptions {
    TString CoordinatorUrl;
    ui32 WorkerId = 0;
};

void SignalHandler(int) {
    isInterrupted = true;
}

int main(int argc, const char *argv[]) {
    try {
        SetInterruptSignalsHandler(SignalHandler);
        TWorkerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption("coordinator-url", "Fast map reduce coordinator server url").Required().StoreResult(&options.CoordinatorUrl);
        opts.AddLongOption("worker-id", "Fast map reduce worker id").Required().StoreResult(&options.WorkerId);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

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

        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(3));
                return ETaskStatus::Completed;
            }
            return ETaskStatus::Aborted;
        }; // TODO - use function which actually calls Downloader/Uploader based on task params
        TFmrJobFactorySettings settings{.Function=func};
        auto jobFactory = MakeFmrJobFactory(settings);
        auto worker = MakeFmrWorker(coordinator, jobFactory , workerSettings);
        worker->Start();

        while (!isInterrupted) {
            Sleep(TDuration::Seconds(1));
        }
        worker->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
