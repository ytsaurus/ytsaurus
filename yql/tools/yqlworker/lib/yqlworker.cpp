#include <yql/tools/yqlworker/lib/yqlworker.h>
#include <yql/tools/yqlworker/yt_runtime/yt_runtime.h>
#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/core/worker_core.h>
#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>
#include <yql/tools/yqlworker/logging/interface.h>

#ifndef _win_
#   include <yql/tools/yqlworker/executor/subproc/bottom_half.h>
#   include <yql/tools/yqlworker/executor/subproc/control_protocol.h>
#   include <yql/tools/yqlworker/dq/worker_dq.h>
#   include <yql/tools/yqlworker/misc/multiprocessing.h>

#endif

#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/utils/sys/become_user.h>

#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/tls_backend.h>
#include <yql/essentials/utils/network/bind_in_range.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/proc_alive.h>
#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_version.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/string/cast.h>
#include <util/system/daemon.h>
#include <util/system/env.h>
#include <util/system/getpid.h>
#include <util/system/mlock.h>
#include <util/system/progname.h>
#include <util/system/file_lock.h>
#include <util/system/guard.h>
#include <util/system/fs.h>
#include <util/folder/path.h>
#include <util/generic/scope.h>

namespace NYql {

namespace {

using namespace NKikimr;
#ifndef _win_
using namespace NSubprocessTaskExecutor;
#endif

enum EChildProcessType {
    CP_CORE,
    CP_FORKER,
    CP_DQ,
    CP_SUPERVISOR,
};

#ifndef _win_
std::pair<TIpcChannelEndpoint, TIpcChannelEndpoint> g_ForkerChannel;
#endif
TBindResultVector g_CoreServerPorts;
TBindResultVector g_CoreTaskServerPorts;
TBindResultVector g_CoreDQServerPorts;

TMaybe<NProto::TDqConfig> GetDqConfig(const TMainConfig& config)
{
    return config.Worker.HasDq()
           ? config.Worker.GetDq().HasPort()
             ? config.Worker.GetDq()
             : [&] () {
                        NProto::TDqConfig dqConfig;
                        dqConfig.MergeFrom(config.Worker.GetDq());
                        dqConfig.SetPort(g_CoreDQServerPorts[2].Addr.GetPort());
                        return dqConfig;
                    } ()
           : TMaybe<NProto::TDqConfig>();
}

void BindCorePorts(TRangeWalker<int>& portWalker)
{
    g_CoreServerPorts = BindInRange(portWalker);
    g_CoreTaskServerPorts = BindInRange(portWalker);
}

// Use different port range for Dq (firewall)
void BindDqPorts(TRangeWalker<int>& portWalker)
{
    auto dqServerPorts = BindInRange(portWalker);
    g_CoreDQServerPorts.insert(g_CoreDQServerPorts.end(), dqServerPorts.begin(), dqServerPorts.end());
    dqServerPorts = BindInRange(portWalker);
    g_CoreDQServerPorts.insert(g_CoreDQServerPorts.end(), dqServerPorts.begin(), dqServerPorts.end());
}

void TryLockMemory(ELockAllMemoryFlags flags)
{
    try {
        LockAllMemory(flags);
    } catch (yexception&) {
        Cerr << "mlockall failed, but that's fine" << Endl;
    }
}

#ifndef _win_
void InitMasterChildAtFork()
{
    ResetSignalFlags();
    CatchInterruptSignal(false);
    SendSignalOnParentThreadExit(SIGTERM);
}

void InitSupervisorChildAtFork(
    const TString& role,
    const TString& procSuffix)
{
    ResetSignalFlags();
    InitSignalsWithSelfPipe();
    TryLockMemory(LockCurrentMemory);
    AllowAnySignals();
    NLog::YqlLogger().UpdateProcInfo(role);
    const TString title = role + " process " + procSuffix;
    SetProcTitle(title.c_str());
}
#endif

int SingleProcessMain(const TMainConfig& config)
{
    ConfigureYtRuntime(/*enableYpDiscovery*/ true);

    ExtLoggingBackends(config.Worker.GetLogging());
    auto oldBackend = NLog::YqlLogger().ReleaseBackend();
    NLog::YqlLogger().ResetBackend(THolder(new NLog::TTlsLogBackend(oldBackend)));

    YQL_LOG(INFO) << "yqlworker started in singleprocess mode";
    YQL_LOG(INFO) << "yqlworker revision: " << GetProgramSvnRevision()
                  << ", UDF ABI version: " << NUdf::CurrentAbiVersionStr();


    // these caps should reach UdfResolver
    if (config.Daemonize && config.Worker.GetExecutor().HasUdfResolverUser()) {
        TurnOnBecomeUserAmbientCaps();
    }

    // reduce privileges
    if (config.Daemonize && config.Worker.HasUser()) {
        BecomeUser(config.Worker.GetUser(), config.Worker.GetGroup());
    }

    InitSignalsWithSelfPipe();
    AllowAnySignals();

    auto execSensors = GetSensorsGroupFor(NSensorComponent::kExecutor);
    auto taskExecutor = CreateInprocessTaskExecutor(
            config.Worker.GetExecutor(),
            config.Worker.HasDq()
                ? config.Worker.GetDq()
                : TMaybe<NProto::TDqConfig>(),
            config.Gateways,
            config.Worker.GetStaticGateways(),
            config.FileStorage,
            CreateMetricsRegistry(execSensors));

    const auto& s = config.Worker.GetServer();
    TRangeWalker<int> portWalker(s.GetPortStart(), s.GetPortFinish());
    BindCorePorts(portWalker);

    if (config.Worker.HasDq()) {
        const auto& s = config.Worker.GetDq();
        TRangeWalker<int> portWalker(s.GetPortStart(), s.GetPortFinish());
        BindDqPorts(portWalker);
    }

    return RunWorkerCore(config, taskExecutor, g_CoreServerPorts);
}

#ifndef _win_
int CoreChildProcessMain(const TMainConfig& config)
{
    YQL_LOG(DEBUG) << "core process starting";

    // reduce privileges
    if (config.Daemonize && (config.Worker.HasUser() || config.Worker.HasCoreUser())) {
        if (config.Worker.HasCoreUser()) {
            BecomeUser(config.Worker.GetCoreUser(), config.Worker.GetCoreGroup());
        } else {
            BecomeUser(config.Worker.GetUser(), config.Worker.GetGroup());
        }
    }
    // use this trick only in multi-process mode
    // it is important to call prctl _after_ setuid
    SendSignalOnParentThreadExit(SIGTERM);
    auto execSensors = GetSensorsGroupFor(NSensorComponent::kExecutor);
    ITaskExecutorPtr taskExecutor = CreateSubprocessTaskExecutor(
                config.Worker.GetExecutor(),
                g_ForkerChannel.second,
                CreateMetricsRegistry(execSensors),
                g_CoreTaskServerPorts);

    return RunWorkerCore(config, taskExecutor, g_CoreServerPorts);
}

int ForkerChildProcessMain(const TMainConfig& config)
{
    YQL_LOG(INFO) << "forker process started";

    // reduce privileges
    if (config.Daemonize && config.Worker.HasUser()) {
        BecomeUser(config.Worker.GetUser(), config.Worker.GetGroup());
    }
    // use this trick only in multi-process mode
    // it is important to call prctl _after_ setuid
    SendSignalOnParentThreadExit(SIGTERM);

    auto execSensors = GetSensorsGroupFor(NSensorComponent::kExecutor);
    TForkerProcess forker(config,
        GetDqConfig(config),
        g_ForkerChannel.first,
        CreateMetricsRegistry(execSensors));
    return forker.Run();
}

int DqChildProcessMain(const TMainConfig& config)
{
    YQL_LOG(INFO) << "dq process started";

    // reduce privileges
    if (config.Daemonize && (config.Worker.HasUser() || config.Worker.GetDq().HasUser())) {
        if (config.Worker.GetDq().HasUser()) {
            BecomeUser(config.Worker.GetDq().GetUser(), config.Worker.GetDq().GetGroup());
        } else {
            BecomeUser(config.Worker.GetUser(), config.Worker.GetGroup());
        }
    }
    // use this trick only in multi-process mode
    // it is important to call prctl _after_ setuid
    // sending quit since dq worker uses two-phase shutdown
    SendSignalOnParentThreadExit(SIGQUIT);

    ConfigureYtRuntime(/*enableYpDiscovery*/ false);
    TWorkerDq dq(config, g_CoreDQServerPorts);
    return dq.Run();
}

int SupervisorProcessMain(TMainConfig& config, const TString& procSuffix)
{
    NLog::YqlLogger().UpdateProcInfo("supervisor");
    TString title = "supervisor process " + procSuffix;
    SetProcTitle(title.data());
    TryLockMemory(LockCurrentMemory);

    // Creating channel here allow to mitigate one side accidental failure.
    // E.g. already running CORE process (with its in-memory state) can continue
    // to communicate with newly restarted FORKER process (after some failure
    // in it) without any issues. And vice versa.
    g_ForkerChannel = DuplexIpcChannel();

    // Preserve worker server ports to make CORE process use the same ports
    // after accidental restart. This will allow already running TASK processes
    // to reestablish connection with new CORE process and continue to send
    // task status updates.
    const auto& s = config.Worker.GetServer();
    TRangeWalker<int> portWalker(s.GetPortStart(), s.GetPortFinish());
    BindCorePorts(portWalker);

    if (config.Worker.HasDq()) {
        const auto& s = config.Worker.GetDq();
        TRangeWalker<int> portWalker(s.GetPortStart(), s.GetPortFinish());
        BindDqPorts(portWalker);
    }

    // these caps should reach UdfResolver
    if (config.Daemonize && config.Worker.GetExecutor().HasUdfResolverUser()) {
        TurnOnBecomeUserAmbientCaps();
    }

    // spawn children processes
    TProcessPool childrenProcesses;
    childrenProcesses.Add(CP_FORKER, "FORKER", 1, [&config, &procSuffix](void*) {
        InitSupervisorChildAtFork("forker", procSuffix);
        g_ForkerChannel.second.Close();
        return ForkerChildProcessMain(config);
    });
    childrenProcesses.Add(CP_CORE, "CORE", 1, [&config, &procSuffix](void*) {
        InitSupervisorChildAtFork("core", procSuffix);
        ExtLoggingBackends(config.Worker.GetLogging());
        g_ForkerChannel.first.Close();
        try {
            return CoreChildProcessMain(config);
        } catch (const TPeerDead&) {
            YQL_LOG(WARN) << "it seems that FORKER process is dead. exiting with code 0";
            return 0;
        }
    });

    const bool hasDq = config.Worker.HasDq() && !config.Worker.GetDq().HasPort();
    if (hasDq) {
        childrenProcesses.Add(CP_DQ, "DQ", 1, [&config, &procSuffix](void*) {
            InitSupervisorChildAtFork("dq", procSuffix);
            ExtLoggingBackends(config.Worker.GetLogging());
            g_ForkerChannel.first.Close();
            return DqChildProcessMain(config);
        });
    }

    childrenProcesses.SpawnChildren();

    sigset_t blockMask;
    SigEmptySet(&blockMask);
    bool terminating = false;
    bool quitting = false;

    // supervisor's main loop
    while (true) {
        EnsureSignalsAreBlocked();
        SigSuspend(&blockMask);

        if (NeedTerminate || NeedQuit) {
            if (!terminating) {
                AddProcTitleSuffix("[terminating]");
                terminating = true;
            }
            quitting = quitting || (NeedQuit && !NeedTerminate);
            // we won't restart FORKER in this state - so close its descriptor
            g_ForkerChannel.first.Close();
            childrenProcesses.Shutdown(NeedTerminate ? SIGTERM : SIGQUIT);
            NeedQuit = NeedTerminate = 0;
        } else if (NeedReopenLog) {
            NeedReopenLog = 0;
            NLog::ReopenLog();
            config.ReopenStdErr();
        } else if (NeedReapZombies) {
            NeedReapZombies = 0;
            TVector<TProcessPool::TExitStatus> statusList;
            childrenProcesses.ReapZombies(&statusList);

            if (terminating) {
                bool forkerExited = AnyOf(statusList, [](const auto& status) { return status.Type == CP_FORKER; });
                if (forkerExited) {
                    YQL_LOG(INFO) << "forker process is exited (while in terminating state) - shutdown DQ";
                    childrenProcesses.Shutdown(SIGQUIT, CP_DQ, /*force =*/true);
                } else if (!quitting) {
                    YQL_LOG(INFO) << "restarting children (while in terminating state)";
                    bool restarted = false;
                    for (auto& status : statusList) {
                        if (status.ExitCode != 0) {
                            if (childrenProcesses.Empty(CP_FORKER)) {
                                // DQ shuts down in two phases: SIGTERM, then SIGQUIT
                                // triggered by FORKER exit. With FORKER gone, a restarted
                                // DQ would hang forever awaiting that SIGQUIT.
                                // CORE also communicates with FORKER via an IPC channel,
                                // so a restarted CORE would face a dead peer too.
                                YQL_LOG(INFO) << "not restarting " << (status.Type == CP_CORE ? "CORE" : "DQ")
                                              << " while terminating (forker is gone)";
                            } else if (status.Type == CP_CORE) {
                                YQL_LOG(INFO) << "restarting CORE while terminating";
                                childrenProcesses.SpawnOneMore(CP_CORE);
                                restarted = true;
                            } else {
                                Y_ENSURE(status.Type == CP_DQ);
                                YQL_LOG(INFO) << "restarting DQ while terminating";
                                childrenProcesses.SpawnOneMore(CP_DQ);
                                restarted = true;
                            }
                        }
                    }

                    if (restarted) {
                        // slow down a little bit busy loop with spawning child processes
                        Sleep(TDuration::Seconds(5));
                        childrenProcesses.Shutdown(SIGTERM); // just in case
                    }
                }
            } else {
                YQL_LOG(INFO) << "restarting children";
                childrenProcesses.SpawnChildren();
                // slow down a little bit busy loop with spawning child processes
                Sleep(TDuration::Seconds(5));
            }
        }

        if (terminating && childrenProcesses.Empty()) {
            break;
        }
    }

    YQL_LOG(INFO) << "terminated";
    return 0;
}

int MultiProcessMain(TMainConfig& config, const TString& procSuffix)
{
    NLog::YqlLogger().UpdateProcInfo("master");
    SetProcTitle("master process");

    YQL_LOG(INFO) << "yqlworker started in multiprocess mode";
    YQL_LOG(INFO) << "yqlworker revision: " << GetProgramSvnRevision()
                  << ", UDF ABI version: " << NUdf::CurrentAbiVersionStr();

    CatchInterruptSignal(true);
    // spawn supervisor process
    TProcessPool childrenProcesses;
    childrenProcesses.Add(CP_SUPERVISOR, "SUPERVISOR", 1, [&config, &procSuffix](void*) {
        InitMasterChildAtFork();
        return SupervisorProcessMain(config, procSuffix);
    });

    childrenProcesses.SpawnChildren();

    sigset_t blockMask;
    SigEmptySet(&blockMask);

    // master's main loop
    while (true) {
        EnsureSignalsAreBlocked();
        SigSuspend(&blockMask);

        if (NeedTerminate) {
            AddProcTitleSuffix("[terminating]");
            // notify children and leave them
            childrenProcesses.Shutdown(SIGTERM);
            childrenProcesses.Detach();
            break;
        } else if (NeedQuit) {
            AddProcTitleSuffix("[terminating]");
            // notify children and wait their termination
            childrenProcesses.Shutdown(SIGQUIT);
            childrenProcesses.Wait();
            break;
        } else if (NeedInterrupt) {
            childrenProcesses.Shutdown(SIGINT);
            childrenProcesses.Detach();
            break;
        } else if (NeedReopenLog) {
            NeedReopenLog = 0;
            NLog::ReopenLog();
            config.ReopenStdErr();
        } else if (NeedReconfigure) {
            NeedReconfigure = 0;
            YQL_LOG(INFO) << "reconfiguring yqlworker";

            config.ParseAgain();

            childrenProcesses.Shutdown(SIGTERM);
            childrenProcesses.SpawnChildren();
        } else if (NeedReapZombies) {
            NeedReapZombies = 0;
            childrenProcesses.ReapZombies();

            // slow down a little bit busy loop with spawning child processes
            Sleep(TDuration::Seconds(5));
            childrenProcesses.SpawnChildren();
        }
    }

    YQL_LOG(INFO) << "terminated";
    return 0;
}
#endif

void CleanupOldPidFiles(TFsPath dir) {
    try {
        dir.MkDirs();
    } catch (...) {
        YQL_LOG(ERROR) << "Cannot create directory for pid files " << dir.GetPath() << ": " << CurrentExceptionMessage();
        throw;
    }
    try {
        TVector<TString> names;
        dir.ListNames(names);

        for (const TString& name: names) {
            TFsPath childPath(dir / name);
            ui32 oldPid;
            if (childPath.IsFile() && TryFromString(name, oldPid) && !IsProcessAlive(oldPid)) {
                TFileLock childLock(childPath);
                if (auto guard = TTryGuard(childLock)) {
                    NFs::Remove(childPath);
                    YQL_LOG(INFO) << "Deleting orphan pid file: " << childPath.GetPath();
                }
            }
        }
    } catch (...) {
        YQL_LOG(WARN) << "Error listing old pid files: " << CurrentExceptionMessage();
    }

}

} // namespace

int RunWorkerMain(int argc, const char* argv[])
{
    SetProgramName(argv[0]);
    TryLockMemory(LockCurrentMemory | LockFutureMemory);

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    ProcTitleInit(argc, argv);
    try {
        Y_UNUSED(NUdf::GetStaticSymbols());
        TMainConfig config;
        config.ParseFromCommandLineArgs(argc, argv);

        if (config.Daemonize) {
            NDaemonMaker::MakeMeDaemon();
        }
        config.ReopenStdErr();

        if (config.Worker.HasDisableUnwinding() && config.Worker.GetDisableUnwinding()) {
            NYql::NBacktrace::DisableBacktraceUnwinding();
        }

        // initialize all loggers except unified agent
        NYql::NLog::InitLogger(config.Worker.GetLogging(), config.Daemonize);
        InitSignals();
        EnsureSignalsAreBlocked();

        if (config.Worker.GetExecutor().HasRunDir()) {
            CleanupOldPidFiles(config.Worker.GetExecutor().GetRunDir());
        }

        auto procSuffix = "(" + ToString(GetPID()) + ")";
#ifdef _win_
        Y_UNUSED(procSuffix);
        // on windows always run in a single process mode
        return SingleProcessMain(config);
#else
        auto mode = config.Worker.GetProcessMode();
        if (mode == NProto::TProcessMode::SINGLE_PROCESS) {
            return SingleProcessMain(config);
        }
        return MultiProcessMain(config, procSuffix);
#endif
    } catch (...) {
        TString message = CurrentExceptionMessage();
        if (NLog::IsYqlLoggerInitialized()) {
            YQL_LOG(FATAL) << message;
        } else {
            Cerr << message << Endl;
        }
        NLog::CleanupLogger();
        return 1;
    }
}

} // namespace NYql
