#include "executor.h"
#include "cypress_executors.h"
#include "transaction_executors.h"
#include "file_executors.h"
#include "table_executors.h"
#include "scheduler_executors.h"
#include "admin_executors.h"

#include <ytlib/logging/log_manager.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/config.h>

#include <ytlib/bus/tcp_dispatcher.h>

#include <ytlib/rpc/rpc_dispatcher.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/ytree/yson_parser.h>

#include <ytlib/misc/crash_handler.h>
#include <ytlib/misc/thread.h>

#include <server/exec_agent/config.h>

#include <util/stream/pipe.h>
#include <util/system/sigset.h>

#include <yt/build.h>

namespace NYT {

using namespace NDriver;
using namespace NYTree;

/////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DriverLogger;

/////////////////////////////////////////////////////////////////////////////

class TDriverProgram
{
public:
    TDriverProgram()
        : ExitCode(0)
    {
        RegisterExecutor(New<TStartTxExecutor>());
        RegisterExecutor(New<TRenewTxExecutor>());
        RegisterExecutor(New<TCommitTxExecutor>());
        RegisterExecutor(New<TAbortTxExecutor>());

        RegisterExecutor(New<TGetExecutor>());
        RegisterExecutor(New<TSetExecutor>());
        RegisterExecutor(New<TRemoveExecutor>());
        RegisterExecutor(New<TListExecutor>());
        RegisterExecutor(New<TCreateExecutor>());
        RegisterExecutor(New<TLockExecutor>());
        RegisterExecutor(New<TCopyExecutor>());
        RegisterExecutor(New<TMoveExecutor>());
        RegisterExecutor(New<TExistsExecutor>());

        RegisterExecutor(New<TDownloadExecutor>());
        RegisterExecutor(New<TUploadExecutor>());

        RegisterExecutor(New<TReadExecutor>());
        RegisterExecutor(New<TWriteExecutor>());

        RegisterExecutor(New<TMapExecutor>());
        RegisterExecutor(New<TMergeExecutor>());
        RegisterExecutor(New<TSortExecutor>());
        RegisterExecutor(New<TEraseExecutor>());
        RegisterExecutor(New<TReduceExecutor>());
        RegisterExecutor(New<TMapReduceExecutor>());
        RegisterExecutor(New<TAbortOpExecutor>());
        RegisterExecutor(New<TTrackOpExecutor>());

        // Admin commands
        RegisterExecutor(New<TBuildSnapshotExecutor>());
    }

    int Main(int argc, const char* argv[])
    {
        NYT::InstallCrashSignalHandler();
        NYT::NThread::SetCurrentThreadName("Driver");

        // Set handler for SIGPIPE.
        SetupSignalHandler();

        try {
            if (argc < 2) {
                PrintAllCommands();
                THROW_ERROR_EXCEPTION("Not enough arguments");
            }
            
            Stroka commandName = Stroka(argv[1]);

            if (commandName == "--help") {
                PrintAllCommands();
                return 0;
            }

            if (commandName == "--version") {
                PrintVersion();
                return 0;
            }

            if (commandName == "--config-template") {
                TYsonWriter writer(&Cout, EYsonFormat::Pretty);
                New<TExecutorConfig>()->Save(&writer);
                return 0;
            }

            auto Executor = GetExecutor(commandName);

            std::vector<std::string> args;
            for (int i = 1; i < argc; ++i) {
                args.push_back(std::string(argv[i]));
            }

            ExitCode = Executor->Execute(args);
        } catch (const std::exception& ex) {
            Cerr << "ERROR: " << ex.what() << Endl;
            ExitCode = EExitCode::Error;
        }

        // TODO: refactor system shutdown
        // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp.
        NLog::TLogManager::Get()->Shutdown();
        NBus::TTcpDispatcher::Get()->Shutdown();
        NRpc::TRpcDispatcher::Get()->Shutdown();
        NChunkClient::TDispatcher::Get()->Shutdown();
        NProfiling::TProfilingManager::Get()->Shutdown();
        TDelayedInvoker::Shutdown();

        return ExitCode;
    }

private:
    int ExitCode;
    yhash_map<Stroka, TExecutorPtr> Executors;

    void SetupSignalHandler()
    {
#ifdef _unix_
        // Set mask.
        sigset_t sigset;
        SigEmptySet(&sigset);
        SigAddSet(&sigset, SIGPIPE);
        SigProcMask(SIG_UNBLOCK, &sigset, NULL);

        // Set handler.
        struct sigaction newAction;
        newAction.sa_handler = SigPipeHandler;
        sigaction(SIGPIPE, &newAction, NULL);
#endif
    }

    static void SigPipeHandler(int signum)
    {
        UNUSED(signum);

        static volatile sig_atomic_t inProgress = 0;
        if (inProgress == 0) {
            inProgress = 1;
            // TODO: refactor system shutdown
            // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp.
            NLog::TLogManager::Get()->Shutdown();
            NBus::TTcpDispatcher::Get()->Shutdown();
            NRpc::TRpcDispatcher::Get()->Shutdown();
            NChunkClient::TDispatcher::Get()->Shutdown();
            NProfiling::TProfilingManager::Get()->Shutdown();
            TDelayedInvoker::Shutdown();
            exit(0);
        }
    }

    void PrintAllCommands()
    {
        printf("Available commands:\n");
        FOREACH (const auto& pair, GetSortedIterators(Executors)) {
            printf("  %s\n", ~pair->first);
        }
    }

    void PrintVersion()
    {
        printf("%s\n", YT_VERSION);
    }

    void RegisterExecutor(TExecutorPtr executor)
    {
        auto name = executor->GetCommandName();
        YCHECK(Executors.insert(MakePair(name, executor)).second);
    }

    TExecutorPtr GetExecutor(const Stroka& commandName)
    {
        auto it = Executors.find(commandName);
        if (it == Executors.end()) {
            THROW_ERROR_EXCEPTION("Unknown command %s", ~commandName.Quote());
        }
        return it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    NYT::TDriverProgram program;
    return program.Main(argc, argv);
}

