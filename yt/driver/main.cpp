#include "executor.h"
#include "cypress_executors.h"
#include "transaction_executors.h"
#include "file_executors.h"
#include "table_executors.h"
#include "scheduler_executors.h"
#include "etc_executors.h"

#include <core/build.h>

#include <core/yson/parser.h>

#include <core/misc/crash_handler.h>
#include <core/misc/collection_helpers.h>

#include <ytlib/shutdown.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/private.h>

#include <ytlib/shutdown.h>

#include <ytlib/shutdown.h>

#include <server/exec_agent/config.h>

#include <util/stream/pipe.h>
#include <util/system/sigset.h>

namespace NYT {

using namespace NDriver;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

/////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DriverLogger;

/////////////////////////////////////////////////////////////////////////////

class TDriverProgram
{
public:
    TDriverProgram()
        : ExitCode(EExitCode::OK)
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
        RegisterExecutor(New<TMountTableExecutor>());
        RegisterExecutor(New<TUnmountTableExecutor>());
        RegisterExecutor(New<TRemountTableExecutor>());
        RegisterExecutor(New<TReshardTableExecutor>());
        RegisterExecutor(New<TInsertExecutor>());
        RegisterExecutor(New<TSelectExecutor>());
        RegisterExecutor(New<TLookupExecutor>());
        RegisterExecutor(New<TDeleteExecutor>());

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
        NYT::NConcurrency::SetCurrentThreadName("Driver");

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

            auto executor = GetExecutor(commandName);

            std::vector<std::string> args;
            for (int i = 1; i < argc; ++i) {
                args.push_back(std::string(argv[i]));
            }

            executor->Execute(args);
        } catch (const std::exception& ex) {
            Cerr << "ERROR: " << ex.what() << Endl;
            ExitCode = EExitCode::Error;
        }

        Shutdown();

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
            Shutdown();
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

