#include "arguments.h"

#include <ytlib/logging/log_manager.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/driver/driver.h>
#include <ytlib/driver/config.h>

#include <ytlib/rpc/rpc_manager.h>

#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/yson_parser.h>

#include <ytlib/exec_agent/config.h>

#include <ytlib/misc/errortrace.h>
#include <ytlib/misc/thread.h>

#include <util/stream/pipe.h>

#include <build.h>

#ifdef _win_
#include <io.h>
#else
#include <unistd.h>
#include <errno.h>
#endif

namespace NYT {

using namespace NDriver;
using namespace NYTree;

static NLog::TLogger& Logger = DriverLogger;

/////////////////////////////////////////////////////////////////////////////

class TDriverProgram
{
public:
    TDriverProgram()
        : ExitCode(0)
    {
        //RegisterParser("start_tx", New<TStartTxArgsParser>());
        //RegisterParser("renew_tx", New<TRenewTxArgsParser>());
        //RegisterParser("commit_tx", New<TCommitTxArgsParser>());
        //RegisterParser("abort_tx", New<TAbortTxArgsParser>());

        RegisterParser("get", New<TGetArgsParser>());
        //RegisterParser("set", New<TSetArgsParser>());
        //RegisterParser("remove", New<TRemoveArgsParser>());
        //RegisterParser("list", New<TListArgsParser>());
        //RegisterParser("create", New<TCreateArgsParser>());
        //RegisterParser("lock", New<TLockArgsParser>());

        //RegisterParser("download", New<TDownloadArgsParser>());
        //RegisterParser("upload", New<TUploadArgsParser>());

        //RegisterParser("read", New<TReadArgsParser>());
        //RegisterParser("write", New<TWriteArgsParser>());

        //RegisterParser("map", New<TMapArgsParser>());
        //RegisterParser("merge", New<TMergeArgsParser>());
        //RegisterParser("sort", New<TSortArgsParser>());
        //RegisterParser("erase", New<TEraseArgsParser>());
        //RegisterParser("abort_op", New<TAbortOpArgsParser>());
    }

    int Main(int argc, const char* argv[])
    {
        NYT::SetupErrorHandler();
        NYT::NThread::SetCurrentThreadName("DriverMain");

        try {
            if (argc < 2) {
                PrintAllCommands();
                ythrow yexception() << "Not enough arguments";
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
                New<TArgsParserBase::TConfig>()->Save(&writer);
                return 0;
            }

            auto argsParser = GetArgsParser(commandName);

            std::vector<std::string> args;
            for (int i = 1; i < argc; ++i) {
                args.push_back(std::string(argv[i]));
            }

            auto error = argsParser->Execute(args);
            if (!error.IsOK()) {
                ExitCode = 1;
            }
        } catch (const std::exception& ex) {
            Cerr << "ERROR: " << ex.what() << Endl;
            ExitCode = 1;
        }

        // TODO: refactor system shutdown
        // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp.
        NLog::TLogManager::Get()->Shutdown();
        NRpc::TRpcManager::Get()->Shutdown();
        NProfiling::TProfilingManager::Get()->Shutdown();
        TDelayedInvoker::Shutdown();

        return ExitCode;
    }

private:
    int ExitCode;
    yhash_map<Stroka, TArgsParserBase::TPtr> ArgsParsers;

    void PrintAllCommands()
    {
        Cout << "Available commands: " << Endl;
        FOREACH (auto parserPair, GetSortedIterators(ArgsParsers)) {
            Cout << "   " << parserPair->first << Endl;
        }
    }

    void PrintVersion()
    {
        Cout << YT_VERSION << Endl;
    }

    void RegisterParser(const Stroka& name, TArgsBasePtr command)
    {
        YVERIFY(ArgsParsers.insert(MakePair(name, command)).second);
    }

    TArgsParserBase::TPtr GetArgsParser(const Stroka& commandName)
    {
        auto parserIt = ArgsParsers.find(commandName);
        if (parserIt == ArgsParsers.end()) {
            ythrow yexception() << Sprintf("Unknown command %s", ~commandName.Quote());
        }
        return parserIt->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    NYT::TDriverProgram program;
    return program.Main(argc, argv);
}

