#include "executor.h"
#include "cypress_executors.h"
#include "transaction_executors.h"
#include "file_executors.h"
#include "table_executors.h"
#include "scheduler_executors.h"
#include "etc_executors.h"
#include "journal_executors.h"

#include <core/build.h>

#include <core/yson/parser.h>

#include <core/misc/at_exit_manager.h>
#include <core/misc/crash_handler.h>
#include <core/misc/collection_helpers.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/private.h>

#include <server/exec_agent/config.h>

#include <util/stream/pipe.h>
#include <util/system/sigset.h>

namespace NYT {

using namespace NDriver;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

/////////////////////////////////////////////////////////////////////////////

class TDriverProgram
{
public:
    TDriverProgram()
        : ExitCode(EExitCode::OK)
    {
#define REGISTER(command, name) \
        RegisterExecutor(New<command>(), name);

        REGISTER(TStartTransactionExecutor,  "start_tx"          );
        REGISTER(TPingTransactionExecutor,   "ping_tx"           );
        REGISTER(TCommitTransactionExecutor, "commit_tx"         );
        REGISTER(TAbortTransactionExecutor,  "abort_tx"          );

        REGISTER(TCreateExecutor,            "create"            );
        REGISTER(TRemoveExecutor,            "remove"            );
        REGISTER(TSetExecutor,               "set"               );
        REGISTER(TGetExecutor,               "get"               );
        REGISTER(TListExecutor,              "list"              );
        REGISTER(TLockExecutor,              "lock"              );
        REGISTER(TCopyExecutor,              "copy"              );
        REGISTER(TMoveExecutor,              "move"              );
        REGISTER(TLinkExecutor,              "link"              );
        REGISTER(TExistsExecutor,            "exists"            );

        REGISTER(TWriteFileExecutor,         "write_file"        );
        REGISTER(TReadFileExecutor,          "read_file"         );
        // COMPAT(babenko)
        REGISTER(TWriteFileExecutor,         "upload"            );
        REGISTER(TReadFileExecutor,          "download"          );

        REGISTER(TWriteTableExecutor,        "write_table"       );
        REGISTER(TReadTableExecutor,         "read_table"        );
        // TODO(babenko): proper naming
        REGISTER(TInsertExecutor,            "insert"            );
        REGISTER(TSelectExecutor,            "select"            );
        REGISTER(TLookupExecutor,            "lookup"            );
        REGISTER(TDeleteExecutor,            "delete"            );
        // COMPAT(babenko)
        REGISTER(TWriteTableExecutor,        "write"             );
        REGISTER(TReadTableExecutor,         "read"              );

        REGISTER(TMountTableExecutor,        "mount_table"       );
        REGISTER(TUnmountTableExecutor,      "unmount_table"     );
        REGISTER(TRemountTableExecutor,      "remount_table"     );
        REGISTER(TReshardTableExecutor,      "reshard_table"     );

        REGISTER(TMergeExecutor,             "merge"             );
        REGISTER(TEraseExecutor,             "erase"             );
        REGISTER(TMapExecutor,               "map"               );
        REGISTER(TSortExecutor,              "sort"              );
        REGISTER(TReduceExecutor,            "reduce"            );
        REGISTER(TMapReduceExecutor,         "map_reduce"        );
        REGISTER(TAbortOperationExecutor,    "abort_op"          );
        REGISTER(TSuspendOperationExecutor,  "suspend_op"        );
        REGISTER(TResumeOperationExecutor,   "resume_op"         );
        REGISTER(TTrackOperationExecutor,    "track_op"          );

        REGISTER(TAddMemberExecutor,         "add_member"        );
        REGISTER(TRemoveMemberExecutor,      "remove_member"     );
        REGISTER(TCheckPermissionExecutor,   "check_permission"  );

        REGISTER(TWriteJournalExecutor,      "write_journal"     );
        REGISTER(TReadJournalExecutor,       "read_journal"      );

        REGISTER(TBuildSnapshotExecutor,     "build_snapshot"    );
#undef REGISTER
    }

    int Main(int argc, const char* argv[])
    {
        NYT::InstallCrashSignalHandler();
        NYT::NConcurrency::SetCurrentThreadName("Driver");

        // Set handler for SIGPIPE.
        SetupSignalHandler();

        try {
            if (argc < 2) {
                PrintAllExecutors();
                THROW_ERROR_EXCEPTION("Not enough arguments");
            }

            Stroka commandName = Stroka(argv[1]);

            if (commandName == "--help") {
                PrintAllExecutors();
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
            exit(0);
        }
    }

    void PrintAllExecutors()
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

    void RegisterExecutor(TExecutorPtr executor, const Stroka& name)
    {
        YCHECK(Executors.insert(std::make_pair(name, executor)).second);
    }

    TExecutorPtr GetExecutor(const Stroka& commandName)
    {
        auto it = Executors.find(commandName);
        if (it == Executors.end()) {
            THROW_ERROR_EXCEPTION("Unknown command %Qv", commandName);
        }
        return it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    NYT::TAtExitManager manager;
    NYT::TDriverProgram program;
    return program.Main(argc, argv);
}

