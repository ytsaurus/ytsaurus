#include "stdafx.h"

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

#include <server/job_proxy/job_proxy.h>
#include <server/job_proxy/stracer.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/shutdown.h>

#include <ytlib/misc/tclap_helpers.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/http_server.h>

#include <ytlib/cgroup/cgroup.h>

#include <core/misc/crash_handler.h>
#include <core/misc/proc.h>

#include <core/tools/tools.h>

#include <core/build.h>

#include <core/profiling/profile_manager.h>

#include <core/tracing/trace_manager.h>

#include <core/logging/log_manager.h>

#include <util/system/sigset.h>
#include <util/folder/dirut.h>

#include <contrib/tclap/tclap/CmdLine.h>

#ifdef _linux_
    #include <sys/resource.h>
#endif

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NElection;
using namespace NScheduler;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Server");

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExitCode,
    ((OK)(0))
    ((OptionsError)(1))
    ((BootstrapError)(2))
    ((ExecutorError)(3))
);

////////////////////////////////////////////////////////////////////////////////

struct TArgsParser
{
public:
    TArgsParser()
        : CmdLine("Command line", ' ', GetVersion())
        , WorkingDirectory("", "working-dir", "working directory", false, "", "DIR")
        , Config("", "config", "configuration file", false, "", "FILE")
        , ConfigTemplate("", "config-template", "print configuration file template")
        , Node("", "node", "start cell node")
        , Master("", "master", "start cell master")
        , DumpMasterSnapshot("", "dump-master-snapshot", "load a given master snapshot and dump its content to stderr", false, "", "FILE")
        , Scheduler("", "scheduler", "start scheduler")
        , JobProxy("", "job-proxy", "start job proxy")
        , JobId("", "job-id", "job id (for job proxy mode)", false, "", "ID")
#ifdef _linux_
        , Tool("", "tool", "tool id", false, "", "ID")
        , Spec("", "spec", "tool spec", false, "", "SPEC")
        , CloseAllFDs("", "close-all-fds", "close all file descriptors")
        , CGroups("", "cgroup", "run in cgroup", false, "")
        , Executor("", "executor", "start a user job")
        , PreparePipes("", "prepare-pipe", "prepare pipe descriptor  (for executor mode)", false, "FD")
        , EnableCoreDump("", "enable-core-dump", "enable core dump (for executor mode)")
        , Uid("", "uid", "set uid  (for executor mode)", false, -1, "NUM")
        , Command("", "command", "command (for executor mode)", false, "", "COMMAND")
#endif
    {
        CmdLine.add(WorkingDirectory);
        CmdLine.add(Config);
        CmdLine.add(ConfigTemplate);
        CmdLine.add(Node);
        CmdLine.add(Master);
        CmdLine.add(DumpMasterSnapshot);
        CmdLine.add(Scheduler);
        CmdLine.add(JobProxy);
        CmdLine.add(JobId);
#ifdef _linux_
        CmdLine.add(Tool);
        CmdLine.add(Spec);
        CmdLine.add(CloseAllFDs);
        CmdLine.add(CGroups);
        CmdLine.add(Executor);
        CmdLine.add(PreparePipes);
        CmdLine.add(EnableCoreDump);
        CmdLine.add(Uid);
        CmdLine.add(Command);
#endif
    }

    TCLAP::CmdLine CmdLine;

    TCLAP::ValueArg<Stroka> WorkingDirectory;
    TCLAP::ValueArg<Stroka> Config;
    TCLAP::SwitchArg ConfigTemplate;
    TCLAP::SwitchArg Node;
    TCLAP::SwitchArg Master;
    TCLAP::ValueArg<Stroka> DumpMasterSnapshot;
    TCLAP::SwitchArg Scheduler;
    TCLAP::SwitchArg JobProxy;
    TCLAP::ValueArg<Stroka> JobId;

#ifdef _linux_
    TCLAP::ValueArg<Stroka> Tool;
    TCLAP::ValueArg<Stroka> Spec;
    TCLAP::SwitchArg CloseAllFDs;
    TCLAP::MultiArg<Stroka> CGroups;
    TCLAP::SwitchArg Executor;
    TCLAP::MultiArg<int> PreparePipes;
    TCLAP::SwitchArg EnableCoreDump;
    TCLAP::ValueArg<int> Uid;
    TCLAP::ValueArg<Stroka> Command;
#endif

};

////////////////////////////////////////////////////////////////////////////////

EExitCode GuardedMain(int argc, const char* argv[])
{
    srand(time(nullptr));

    NYT::NConcurrency::SetCurrentThreadName("Bootstrap");

    TArgsParser parser;

    parser.CmdLine.parse(argc, argv);

    // Figure out the mode: cell master, cell node, scheduler or job proxy.
    bool isMaster = parser.Master.getValue();
    bool isMasterSnapshotDump =  parser.DumpMasterSnapshot.isSet();
    bool isNode = parser.Node.getValue();
    bool isScheduler = parser.Scheduler.getValue();
    bool isJobProxy = parser.JobProxy.getValue();

#ifdef _linux_
    Stroka toolName = parser.Tool.getValue();
    bool isExecutor = parser.Executor.getValue();
    bool doCloseAllFDs = parser.CloseAllFDs.getValue();
#endif

    bool printConfigTemplate = parser.ConfigTemplate.getValue();

    Stroka configFileName = parser.Config.getValue();

    Stroka workingDirectory = parser.WorkingDirectory.getValue();

    int modeCount = 0;
    if (isNode) {
        ++modeCount;
    }
    if (isMaster) {
        ++modeCount;
    }
    if (isMasterSnapshotDump) {
        ++modeCount;
    }
    if (isScheduler) {
        ++modeCount;
    }
    if (isJobProxy) {
        ++modeCount;
    }

#ifdef _linux_
    if (!toolName.Empty()) {
        ++modeCount;
    }
    if (isExecutor) {
        ++modeCount;
    }
#endif

    if (modeCount != 1) {
        TCLAP::StdOutput().usage(parser.CmdLine);
        return EExitCode::OptionsError;
    }

#ifdef _linux_
    if (doCloseAllFDs) {
        CloseAllDescriptors();
    }
#endif

    if (!workingDirectory.empty()) {
        ChDir(workingDirectory);
    }

#ifdef _linux_
    if (!toolName.Empty()) {
        NYson::TYsonString spec(parser.Spec.getValue());
        auto result = ExecuteTool(toolName, spec);
        Cout << result.Data();
        // NB: no shutdown, some initialization may still be in progress.
        Cout.Flush();
        _exit(static_cast<int>(EExitCode::OK));
    }
#endif

    INodePtr configNode;

    if (!printConfigTemplate) {
        if (configFileName.empty()) {
            THROW_ERROR_EXCEPTION("Missing --config option");
        }

        // Parse configuration file.
        try {
            TIFStream configStream(configFileName);
            configNode = ConvertToNode(&configStream);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error reading server configuration")
                << ex;
        }

        // Deserialize as a generic server config.
        auto config = New<TServerConfig>();
        config->Load(configNode);

        // Configure singletons.
        NLogging::TLogManager::Get()->Configure(configFileName, "/logging");
        TAddressResolver::Get()->Configure(config->AddressResolver);
        NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
        NTracing::TTraceManager::Get()->Configure(configFileName, "/tracing");
        NProfiling::TProfileManager::Get()->Start();
    }

#ifdef _linux_
    bool enableCGroups = true;
    if (isNode) {
        auto config = New<NCellNode::TCellNodeConfig>();

        try {
            config->Load(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell node configuration")
                << ex;
        }
        enableCGroups = config->ExecAgent->EnableCGroups;
    }

    auto cgroups = parser.CGroups.getValue();
    if (enableCGroups) {
        for (const auto& path : cgroups) {
            NCGroup::TNonOwningCGroup cgroup(path);
            cgroup.EnsureExistance();
            cgroup.AddCurrentTask();
        }
    } else {
        if (!cgroups.empty()) {
            LOG_WARNING("CGroups are explicitely disabled in config; ignoring --cgroup parameter");
        }
    }

    if (isExecutor) {
        NConcurrency::SetCurrentThreadName("ExecutorMain");

        const int permissions = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
        for (auto fd : parser.PreparePipes.getValue()) {
            SetPermissions(fd, permissions);
        }

        if (!parser.EnableCoreDump.getValue()) {
            // Disable core dump for user jobs when option is not present.
            struct rlimit rlimit = {0, 0};

            auto res = setrlimit(RLIMIT_CORE, &rlimit);
            if (res) {
                auto errorMessage = Format("Failed to disable core dumps\n%v", TError::FromSystem());
                fprintf(stderr, "%s", ~errorMessage);
                return EExitCode::ExecutorError;
            }
        }

        auto uid = parser.Uid.getValue();
        if (uid > 0) {
            // Set unprivileged uid and gid for user process.
            YCHECK(setuid(0) == 0);

            YCHECK(setresgid(uid, uid, uid) == 0);
            YCHECK(setuid(uid) == 0);
        }

        auto command = parser.Command.getValue();
        execl("/bin/sh",
            "/bin/sh",
            "-c",
            ~command,
            (void*)NULL);
        return EExitCode::ExecutorError;
    }
#endif

    // Start an appropriate server.
    if (isNode) {
        NConcurrency::SetCurrentThreadName("NodeMain");

        auto config = New<NCellNode::TCellNodeConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell node configuration")
                << ex;
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellNode::TBootstrap(configFileName, config);
        bootstrap->Run();
    }

    if (isMaster || isMasterSnapshotDump) {
        NConcurrency::SetCurrentThreadName("MasterMain");

        auto config = New<NCellMaster::TCellMasterConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell master configuration")
                << ex;
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellMaster::TBootstrap(configFileName, config);
        bootstrap->Initialize();
        if (isMaster) {
            bootstrap->Run();
        } else if (isMasterSnapshotDump) {
            bootstrap->DumpSnapshot(parser.DumpMasterSnapshot.getValue());
        } else {
            YUNREACHABLE();
        }
    }

    if (isScheduler) {
        NConcurrency::SetCurrentThreadName("SchedulerMain");

        auto config = New<NCellScheduler::TCellSchedulerConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(configNode);
            config->Validate();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell scheduler configuration")
                << ex;
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellScheduler::TBootstrap(configFileName, config);
        bootstrap->Run();
    }

    if (isJobProxy) {
        NConcurrency::SetCurrentThreadName("JobProxyMain");

        auto config = New<NJobProxy::TJobProxyConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        TJobId jobId;
        try {
            jobId = TGuid::FromString(parser.JobId.getValue());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing job id")
                << ex;
        }

        try {
            config->Load(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing job proxy configuration")
                << ex;
        }

        auto jobProxy = New<TJobProxy>(config, jobId);
        jobProxy->Run();
    }

    return EExitCode::OK;
}

EExitCode Main(int argc, const char* argv[])
{
    InstallCrashSignalHandler();

#ifdef _unix_
    sigset_t sigset;
    SigEmptySet(&sigset);
    SigAddSet(&sigset, SIGHUP);
    SigProcMask(SIG_BLOCK, &sigset, NULL);

    signal(SIGPIPE, SIG_IGN);

#ifndef _darwin_
    uid_t ruid, euid, suid;
    YCHECK(getresuid(&ruid, &euid, &suid) == 0);
    if (euid == 0) {
        // if effective uid == 0 (e. g. set-uid-root), make
        // saved = effective, effective = real
        YCHECK(setresuid(ruid, ruid, euid) == 0);
    }
#endif /* ! _darwin_ */
#endif /* _unix_ */

    EExitCode exitCode;
    try {
        exitCode = GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Server startup failed");
        exitCode = EExitCode::BootstrapError;
    }

    Shutdown();

    return exitCode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    return static_cast<int>(NYT::Main(argc, argv));
}
