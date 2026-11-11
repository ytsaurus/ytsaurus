#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/dq/worker_dq.h>
#include <yql/tools/yqlworker/executor/subproc/bottom_half.h>
#include <yql/tools/yqlworker/executor/task_executor.h>
#include <yql/tools/yqlworker/lib/yqlworker.h>
#include <yql/tools/yqlworker/yt_runtime/yt_runtime.h>
#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>
#include <yql/tools/yqlworker/misc/socket_transfer.h>

#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_version.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/utils/sys/become_user.h>

#include <library/cpp/messagebus/network.h>
#include <library/cpp/svnversion/svnversion.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/system/getpid.h>
#include <util/system/mlock.h>
#include <util/system/progname.h>

namespace NYql {

namespace {

static const TString CoreSocketName = "core.socket";
static const TString ForkerSocketName = "forker.socket";

TMaybe<NProto::TDqConfig> GetDqConfig(const TMainConfig& config)
{
    if (!config.Worker.HasDq()) {
        return {};
    }
    NProto::TDqConfig dqConfig;
    dqConfig.MergeFrom(config.Worker.GetDq());
    dqConfig.SetPort(config.Worker.GetQt().GetDqGrpcPort());
    return dqConfig;
}

int RunQtMaster(TMainConfig& config) {
    NLog::YqlLogger().UpdateProcInfo("master");
    SetProcTitle("master process");

    YQL_LOG(INFO) << "qtworker " << *config.QtRole << " started";
    YQL_LOG(INFO) << "revision: " << GetProgramSvnRevision()
                  << ", UDF ABI version: " << NUdf::CurrentAbiVersionStr();

    InitSignals();

    TFsPath socketDir(config.Worker.GetQt().GetSocketDir());
    socketDir.MkDirs(0700);

    auto socketPair = DuplexIpcChannel();
    TSocketHolder coreSocket = socketPair.first.Release();
    TSocketHolder forkerSocket = socketPair.second.Release();

    const TFsPath corePath = socketDir / TFsPath(CoreSocketName);
    const TFsPath forkerPath = socketDir / TFsPath(ForkerSocketName);

    TSocketTransferServer coreServer(MakeLocalSocketAddress(corePath), std::move(coreSocket));
    TSocketTransferServer forkerServer(MakeLocalSocketAddress(forkerPath), std::move(forkerSocket));

    Y_DEFER {
        forkerServer.Stop();
        coreServer.Stop();
        unlink(corePath.GetPath().c_str());
        unlink(forkerPath.GetPath().c_str());
    };

    coreServer.Start();
    forkerServer.Start();

    YQL_LOG(INFO) << "Servicing core socket on " << corePath;
    YQL_LOG(INFO) << "Servicing forker socket on " << forkerPath;

    sigset_t blockMask;
    SigEmptySet(&blockMask);

    // master's main loop
    while (true) {
        SigSuspend(&blockMask);
        if (NeedTerminate || NeedQuit || NeedInterrupt) {
            AddProcTitleSuffix("[terminating]");
            // TODO: more efficient handling of NeedReconfigure
            break;
        } else if (NeedReopenLog) {
            NeedReopenLog = 0;
            NLog::ReopenLog();
            config.ReopenStdErr();
        }
    }

    YQL_LOG(INFO) << "terminated";
    return 0;
}

int RunQtCore(TMainConfig& config) {
    //NYql::NLog::AddUnifiedAgentLogger(config.Worker.GetLogging());
    NLog::YqlLogger().UpdateProcInfo("core");
    SetProcTitle("core process");

    YQL_LOG(DEBUG) << "core process starting";

    try {
        InitSignals();
        EnsureSignalsAreBlocked();
        TFsPath socketDir(config.Worker.GetQt().GetSocketDir());

        auto socketAddr = MakeLocalSocketAddress(socketDir / TFsPath(CoreSocketName));

        TIpcChannelEndpoint forkerChannel(ReceiveSocket(socketAddr.Get()).Release());
        YQL_LOG(INFO) << "Got forker channel socket";

        const auto& qt = config.Worker.GetQt();
        auto corePorts = config.CoreBindAll
            ? NBus::BindOnPort(qt.GetCorePort(), /* reusePort = */ false).second
            : NBus::BindOnLoopbackPort(qt.GetCorePort(), /* reusePort = */ false).second;
        YQL_LOG(INFO) << "Listening core port " << config.Worker.GetQt().GetCorePort();

        TVector<NBus::TBindResult> coreTaskPorts = NBus::BindOnLoopbackPort(config.Worker.GetQt().GetCoreTaskPort(), /* reusePort = */ false).second;
        YQL_LOG(INFO) << "Listening task port " << config.Worker.GetQt().GetCoreTaskPort();

        auto execSensors = GetSensorsGroupFor(NSensorComponent::kExecutor);
        ITaskExecutorPtr taskExecutor = CreateSubprocessTaskExecutor(
                    config.Worker.GetExecutor(),
                    forkerChannel,
                    CreateMetricsRegistry(execSensors),
                    coreTaskPorts);

        return RunWorkerCore(config, taskExecutor, corePorts);
    } catch (const TPeerDead&) {
        YQL_LOG(WARN) << "it seems that FORKER process is dead. exiting with code 0";
        return 0;
    }
}

int RunQtForker(TMainConfig& config) {
    InitSignalsWithSelfPipe();
    AllowAnySignals();
    NLog::YqlLogger().UpdateProcInfo("forker");
    SetProcTitle("forker process");
    YQL_LOG(INFO) << "forker process started";

    TFsPath socketDir(config.Worker.GetQt().GetSocketDir());

    auto socketAddr = MakeLocalSocketAddress(socketDir / TFsPath(ForkerSocketName));

    TIpcChannelEndpoint coreChannel(ReceiveSocket(socketAddr.Get()).Release());
    YQL_LOG(INFO) << "Got core channel socket";

    auto execSensors = GetSensorsGroupFor(NSensorComponent::kExecutor);

    NSubprocessTaskExecutor::TForkerProcess forker(config,
        GetDqConfig(config),
        coreChannel,
        CreateMetricsRegistry(execSensors));
    return forker.Run();

}

int RunQtDq(TMainConfig& config) {
    //NYql::NLog::AddUnifiedAgentLogger(config.Worker.GetLogging());
    NLog::YqlLogger().UpdateProcInfo("dq");
    SetProcTitle("dq process");
    if (!config.Worker.HasDq()) {
        YQL_LOG(INFO) << "No dq configuration found, exiting";
        Sleep(TDuration::Seconds(1));
        return 0;
    }

    YQL_LOG(INFO) << "dq process started";

    ConfigureYtRuntime(/*enableYpDiscovery*/ false);

    auto dqPorts = NBus::BindOnPort(config.Worker.GetQt().GetDqInterconnectPort(), /* reusePort = */ false).second;
    auto grpcPorts = NBus::BindOnPort(config.Worker.GetQt().GetDqGrpcPort(), /* reusePort = */ false).second;
    dqPorts.insert(dqPorts.end(), grpcPorts.begin(), grpcPorts.end());

    TWorkerDq dq(config, dqPorts);
    return dq.Run();
}

int RunQtWorkerMain(int argc, const char* argv[]) {
    SetProgramName(argv[0]);

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    ProcTitleInit(argc, argv);
    try {
        Y_UNUSED(NUdf::GetStaticSymbols());
        TMainConfig config(/* forQt = */true);
        config.ParseFromCommandLineArgs(argc, argv);

        if (config.QtRole == EQtWorkerRole::ROLE_FORKER) {
            if (config.Worker.GetExecutor().HasUdfResolverUser()) {
                TurnOnBecomeUserAmbientCaps();
            }
            if (config.Worker.HasUser()) {
                BecomeUser(config.Worker.GetUser(), config.Worker.GetGroup());
                EnsureNonRootUserAndGroup();
            }
        }

        // initialize all loggers except unified agent
        NYql::NLog::InitLogger(config.Worker.GetLogging(), config.Daemonize);
        config.ReopenStdErr();

        Y_ENSURE(config.QtRole.Defined());
        Y_ENSURE(!config.Daemonize);

        auto mode = config.Worker.GetProcessMode();
        if (mode != NProto::TProcessMode::MULTI_PROCESS) {
            ythrow yexception() << "Invalid worker config: only multi process mode is supported";
        }

        if (!config.Worker.HasQt()) {
            ythrow yexception() << "Invalid worker config: Missing Qt section";
        }

        if (config.Worker.HasDisableUnwinding() && config.Worker.GetDisableUnwinding()) {
            NYql::NBacktrace::DisableBacktraceUnwinding();
        }

        CatchInterruptSignal(true);
        switch (*config.QtRole) {
            case EQtWorkerRole::ROLE_MASTER:
                return RunQtMaster(config);
            case EQtWorkerRole::ROLE_CORE:
                return RunQtCore(config);
            case EQtWorkerRole::ROLE_FORKER:
                return RunQtForker(config);
            case EQtWorkerRole::ROLE_DQ:
                return RunQtDq(config);
        }
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

} // namespace

} // namespace NYql

int main(int argc, const char* argv[]) {
    return NYql::RunQtWorkerMain(argc, argv);
}
