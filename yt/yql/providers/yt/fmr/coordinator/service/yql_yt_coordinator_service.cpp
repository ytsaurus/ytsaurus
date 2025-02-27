#include <library/cpp/getopt/last_getopt.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>

using namespace NYql::NFmr;

volatile sig_atomic_t isInterrupted = 0;

struct TCoordinatorServerRunOptions {
    ui16 Port = 7000;
    TString Host = "localhost";
    ui32 WorkersNum = 1;
};

void SignalHandler(int) {
    isInterrupted = 1;
}

int main(int argc, const char *argv[]) {
    try {
        SetInterruptSignalsHandler(SignalHandler);
        TCoordinatorServerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption("port", "Fast map reduce coordinator server port").Optional().StoreResult(&options.Port);
        opts.AddLongOption("host", "Fast map reduce coordinator server host").Optional().StoreResult(&options.Host);
        opts.AddLongOption("workers-num", "Number of fast map reduce workers").Optional().StoreResult(&options.WorkersNum);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        TFmrCoordinatorSettings coordinatorSettings{.WorkersNum = options.WorkersNum, .RandomProvider = CreateDefaultRandomProvider()};
        TFmrCoordinatorServerSettings coordinatorServerSettings{.Port = options.Port, .Host = options.Host};
        auto coordinator = MakeFmrCoordinator(coordinatorSettings);
        auto coordinatorServer = MakeFmrCoordinatorServer(coordinator, coordinatorServerSettings);
        coordinatorServer->Start();

        while (!isInterrupted) {
            Sleep(TDuration::Seconds(1));
        }
        coordinatorServer->Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
