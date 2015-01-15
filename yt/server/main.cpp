#include "stdafx.h"

#include <core/misc/crash_handler.h>
#include <core/misc/address.h>
#include <core/misc/proc.h>

#include <core/build.h>

#include <core/logging/log_manager.h>

#include <core/profiling/profile_manager.h>

#include <core/tracing/trace_manager.h>

#include <core/ytree/yson_serializable.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/shutdown.h>

#include <ytlib/misc/tclap_helpers.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <server/data_node/config.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>
#include <server/cell_node/bootstrap.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

#include <server/job_proxy/config.h>
#include <server/job_proxy/job_proxy.h>

#include <tclap/CmdLine.h>

#include <util/system/sigset.h>
#include <util/system/execpath.h>
#include <util/folder/dirut.h>

#include <contrib/tclap/tclap/CmdLine.h>

#ifdef _linux_
    #include <sys/resource.h>

    #include <core/misc/ioprio.h>

    #include <ytlib/cgroup/cgroup.h>
#endif

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NElection;
using namespace NScheduler;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Server");

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
        , CellNode("", "node", "start cell node")
        , CellMaster("", "master", "start cell master")
        , Scheduler("", "scheduler", "start scheduler")
        , JobProxy("", "job-proxy", "start job proxy")
        , JobId("", "job-id", "job id (for job proxy mode)", false, "", "ID")
#ifdef _linux_
        , Cleaner("", "cleaner", "start cleaner")
        , Killer("", "killer", "start killer")
        , CloseAllFds("", "close-all-fds", "close all file descriptors")
        , DirToRemove("", "dir-to-remove", "directory to remove (for cleaner mode)", false, "", "DIR")
        , ProcessGroupPath("", "process-group-path", "path to process group to kill (for killer mode)", false, "", "UID")
        , CGroups("", "cgroup", "run in cgroup", false, "")
        , Executor("", "executor", "start a user job")
        , PreparePipes("", "prepare-pipe", "prepare pipe descriptor  (for executor mode)", false, "FD")
        , EnableCoreDump("", "enable-core-dump", "enable core dump (for executor mode)")
        , Uid("", "uid", "set uid  (for executor mode)", false, -1, "NUM")
        , EnableIOPrio("", "enable-io-prio", "set low io prio (for executor mode)")
        , Command("", "command", "command (for executor mode)", false, "", "COMMAND")
#endif
    {
        CmdLine.add(WorkingDirectory);
        CmdLine.add(Config);
        CmdLine.add(ConfigTemplate);
        CmdLine.add(CellNode);
        CmdLine.add(CellMaster);
        CmdLine.add(Scheduler);
        CmdLine.add(JobProxy);
        CmdLine.add(JobId);
#ifdef _linux_
        CmdLine.add(Cleaner);
        CmdLine.add(Killer);
        CmdLine.add(CloseAllFds);
        CmdLine.add(DirToRemove);
        CmdLine.add(ProcessGroupPath);
        CmdLine.add(CGroups);
        CmdLine.add(Executor);
        CmdLine.add(PreparePipes);
        CmdLine.add(EnableCoreDump);
        CmdLine.add(Uid);
        CmdLine.add(EnableIOPrio);
        CmdLine.add(Command);
#endif
    }

    TCLAP::CmdLine CmdLine;

    TCLAP::ValueArg<Stroka> WorkingDirectory;
    TCLAP::ValueArg<Stroka> Config;
    TCLAP::SwitchArg ConfigTemplate;
    TCLAP::SwitchArg CellNode;
    TCLAP::SwitchArg CellMaster;
    TCLAP::SwitchArg Scheduler;
    TCLAP::SwitchArg JobProxy;
    TCLAP::ValueArg<Stroka> JobId;

#ifdef _linux_
    TCLAP::SwitchArg Cleaner;
    TCLAP::SwitchArg Killer;
    TCLAP::SwitchArg CloseAllFds;
    TCLAP::ValueArg<Stroka> DirToRemove;
    TCLAP::ValueArg<Stroka> ProcessGroupPath;
    TCLAP::MultiArg<Stroka> CGroups;
    TCLAP::SwitchArg Executor;
    TCLAP::MultiArg<int> PreparePipes;
    TCLAP::SwitchArg EnableCoreDump;
    TCLAP::ValueArg<int> Uid;
    TCLAP::SwitchArg EnableIOPrio;
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
    bool isCellMaster = parser.CellMaster.getValue();
    bool isCellNode = parser.CellNode.getValue();
    bool isScheduler = parser.Scheduler.getValue();
    bool isJobProxy = parser.JobProxy.getValue();

#ifdef _linux_
    bool isCleaner = parser.Cleaner.getValue();
    bool isKiller = parser.Killer.getValue();
    bool isExecutor = parser.Executor.getValue();
    bool doCloseAllFds = parser.CloseAllFds.getValue();
#endif

    bool printConfigTemplate = parser.ConfigTemplate.getValue();

    Stroka configFileName = parser.Config.getValue();

    Stroka workingDirectory = parser.WorkingDirectory.getValue();

    int modeCount = 0;
    if (isCellNode) {
        ++modeCount;
    }
    if (isCellMaster) {
        ++modeCount;
    }
    if (isScheduler) {
        ++modeCount;
    }
    if (isJobProxy) {
        ++modeCount;
    }

#ifdef _linux_
    if (isCleaner) {
        ++modeCount;
    }
    if (isKiller) {
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
    if (doCloseAllFds) {
        CloseAllDescriptors();
    }
#endif

    if (!workingDirectory.empty()) {
        ChDir(workingDirectory);
    }

#ifdef _linux_
    if (isCleaner) {
        NConcurrency::SetCurrentThreadName("CleanerMain");

        Stroka path = parser.DirToRemove.getValue();
        if (path.empty() || path[0] != '/') {
            THROW_ERROR_EXCEPTION("A path should be absolute. Path: %v", ~path);
        }
        int counter = 0;
        size_t nextSlash = 0;
        while (nextSlash != Stroka::npos) {
            nextSlash = path.find('/', nextSlash + 1);
            ++counter;
        }

        if (counter <= 3) {
            THROW_ERROR_EXCEPTION("A path should contain at least 4 slashes. Path: %v", ~path);
        }

        RemoveDirAsRoot(path);

        return EExitCode::OK;
    }

    if (isKiller) {
        NConcurrency::SetCurrentThreadName("KillerMain");

        YCHECK(setuid(0) == 0);
        auto path = parser.ProcessGroupPath.getValue();
        NCGroup::TNonOwningCGroup group(path);
        group.Kill();

        return EExitCode::OK;
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
        NLog::TLogManager::Get()->Configure(configFileName, "/logging");
        TAddressResolver::Get()->Configure(config->AddressResolver);
        NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
        NTracing::TTraceManager::Get()->Configure(configFileName, "/tracing");
        NProfiling::TProfileManager::Get()->Start();
    }

#ifdef _linux_
    bool enableCGroups = true;
    if (isCellNode) {
        auto config = New<NCellNode::TCellNodeConfig>();

        try {
            config->Load(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell node configuration")
                << ex;
        }
        enableCGroups = config->ExecAgent->SlotManager->EnableCGroups;
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
            LOG_WARNING("CGroups are explicitely disabled in config. Ignore --cgroup parameter");
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
                fprintf(stderr, "Failed to disable core dumps\n%s", strerror(errno));
                return EExitCode::ExecutorError;
            }
        }

        auto uid = parser.Uid.getValue();
        if (uid > 0) {
            // Set unprivileged uid and gid for user process.
            YCHECK(setuid(0) == 0);

            YCHECK(setresgid(uid, uid, uid) == 0);
            YCHECK(setuid(uid) == 0);

            if (parser.EnableIOPrio.getValue()) {
                YCHECK(ioprio_set(IOPRIO_WHO_USER, uid, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7)) == 0);
            }
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
    if (isCellNode) {
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

    if (isCellMaster) {
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
        bootstrap->Run();
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

    // If you ever try to remove this I will kill you. I promise. /@babenko
    GetExecPath();

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
