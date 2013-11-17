#include "executor.h"
#include "cypress_executors.h"
#include "transaction_executors.h"
#include "file_executors.h"
#include "table_executors.h"
#include "scheduler_executors.h"
#include "etc_executors.h"

#include <core/misc/address.h>

#include <core/logging/log_manager.h>

#include <core/profiling/profiling_manager.h>

#include <core/concurrency/delayed_executor.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/rpc/dispatcher.h>

#include <ytlib/driver/private.h>
#include <ytlib/driver/driver.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/dispatcher.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <core/yson/parser.h>

#include <core/misc/crash_handler.h>

#include <server/exec_agent/config.h>

#include <util/stream/pipe.h>
#include <util/system/sigset.h>

#include <core/build.h>

namespace NYT {

using namespace NDriver;
using namespace NYTree;
using namespace NYson;

/////////////////////////////////////////////////////////////////////////////

static auto& Logger = DriverLogger;

/////////////////////////////////////////////////////////////////////////////

class TDriverProgram
{
public:
    TDriverProgram()
        : ExitCode(0)
    {
        RegisterExecutor(New<TStartTxExecutor>());
        RegisterExecutor(New<TPingTxExecutor>());
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
        RegisterExecutor(New<TLinkExecutor>());

        RegisterExecutor(New<TDownloadExecutor>());
        RegisterExecutor(New<TUploadExecutor>());

        RegisterExecutor(New<TReadExecutor>());
        RegisterExecutor(New<TWriteExecutor>());
        RegisterExecutor(New<TMountExecutor>());
        RegisterExecutor(New<TUnmountExecutor>());
        RegisterExecutor(New<TInsertExecutor>());
        RegisterExecutor(New<TSelectExecutor>());
        RegisterExecutor(New<TLookupExecutor>());

        RegisterExecutor(New<TMapExecutor>());
        RegisterExecutor(New<TMergeExecutor>());
        RegisterExecutor(New<TSortExecutor>());
        RegisterExecutor(New<TEraseExecutor>());
        RegisterExecutor(New<TReduceExecutor>());
        RegisterExecutor(New<TMapReduceExecutor>());
        RegisterExecutor(New<TAbortOpExecutor>());
        RegisterExecutor(New<TSuspendOpExecutor>());
        RegisterExecutor(New<TResumeOpExecutor>());
        RegisterExecutor(New<TTrackOpExecutor>());

        RegisterExecutor(New<TBuildSnapshotExecutor>());
        RegisterExecutor(New<TGCCollectExecutor>());
        RegisterExecutor(New<TAddMemberExecutor>());
        RegisterExecutor(New<TRemoveMemberExecutor>());
        RegisterExecutor(New<TCheckPermissionExecutor>());
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

        // TODO(sandello): Refactor this.
        // XXX(sandello): Keep in sync with...
        //   server/main.cpp
        //   driver/main.cpp
        //   unittests/utmain.cpp
        //   nodejs/src/common.cpp
        //   ../python/yt/bindings/shutdown.cpp
        // Feel free to add your cpp here. Welcome to the Shutdown Club!

        NDriver::TDispatcher::Get()->Shutdown();
        NBus::TTcpDispatcher::Get()->Shutdown();
        NRpc::TDispatcher::Get()->Shutdown();
        NChunkClient::TDispatcher::Get()->Shutdown();
        NProfiling::TProfilingManager::Get()->Shutdown();
        NConcurrency::TDelayedExecutor::Shutdown();
        TAddressResolver::Get()->Shutdown();
        NLog::TLogManager::Get()->Shutdown();

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

        static volatile sig_atomic_t inProgress = false;
        if (!inProgress) {
            inProgress = true;
            // TODO: refactor system shutdown
            // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp.
            NDriver::TDispatcher::Get()->Shutdown();
            NBus::TTcpDispatcher::Get()->Shutdown();
            NRpc::TDispatcher::Get()->Shutdown();
            NChunkClient::TDispatcher::Get()->Shutdown();
            NProfiling::TProfilingManager::Get()->Shutdown();
            NConcurrency::TDelayedExecutor::Shutdown();
            TAddressResolver::Get()->Shutdown();
            NLog::TLogManager::Get()->Shutdown();
            exit(0);
        }
    }

    void PrintAllCommands()
    {
        printf("Available commands:\n");
        for (const auto& pair : GetSortedIterators(Executors)) {
            printf("  %s\n", ~pair->first);
        }
    }

    void PrintVersion()
    {
        printf("%s\n", GetVersion());
    }

    void RegisterExecutor(TExecutorPtr executor)
    {
        auto name = executor->GetCommandName();
        YCHECK(Executors.insert(std::make_pair(name, executor)).second);
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

