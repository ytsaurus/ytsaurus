#include <library/cpp/getopt/last_getopt.h>
#include <util/system/interrupt_signals.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/mem_limit.h>

using namespace NYql::NFmr;
using namespace NYql;

volatile sig_atomic_t isInterrupted = 0;

class TCoordinatorServerRunOptions {
public:
    ui16 Port;
    TString Host;
    ui32 WorkersNum;
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
        TCoordinatorServerRunOptions options;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();
        opts.AddLongOption('p', "port", "Fast map reduce coordinator server port").StoreResult(&options.Port).DefaultValue(7000);
        opts.AddLongOption('h', "host", "Fast map reduce coordinator server host").StoreResult(&options.Host).DefaultValue("localhost");
        opts.AddLongOption('w', "workers-num", "Number of fast map reduce workers").StoreResult(&options.WorkersNum).DefaultValue(1);
        opts.AddLongOption('v', "verbosity", "Logging verbosity level").StoreResult(&options.Verbosity).DefaultValue(static_cast<int>(TLOG_ERR));
        opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, SetAddressSpaceLimit);
        opts.SetFreeArgsMax(0);

        auto res = NLastGetopt::TOptsParseResult(&opts, argc, argv);

        options.InitLogger();

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
