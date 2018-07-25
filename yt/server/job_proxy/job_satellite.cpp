#include "job_satellite.h"
#include "job_prober_service.h"
#include "job_satellite_connection.h"
#include "user_job.h"
#include "user_job_synchronizer.h"
#include "private.h"

#include <yt/server/exec_agent/public.h>

#include <yt/server/shell/shell_manager.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/process.h>

#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/server.h>

#include <yt/core/tools/tools.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/stracer.h>
#include <yt/core/misc/signaler.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/misc/shutdown.h>

#include <util/generic/guid.h>
#include <util/system/fs.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/ioctl.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NYT::NBus;
using namespace NConcurrency;
using namespace NShell;
using namespace NTools;

using NChunkClient::TChunkId;
using NYTree::INodePtr;
using NJobTrackerClient::TJobId;
using NYson::TYsonString;
using NJobProberClient::IJobProbe;
using NExecAgent::EJobEnvironmentType;

static const NLogging::TLogger JobSatelliteLogger("JobSatellite");
static const auto& Logger = JobSatelliteLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobProbeTools)

////////////////////////////////////////////////////////////////////////////////

struct IPidsHolder
{
    virtual ~IPidsHolder() = default;
    virtual std::vector<int> GetPids() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFreezerPidsHolder
    : public IPidsHolder
{
public:
    explicit TFreezerPidsHolder(const TString& name)
        : Freezer_(name)
    {
        Freezer_.Create();
    }

    virtual std::vector<int> GetPids() override
    {
        return Freezer_.GetTasks();
    }

private:
    NCGroup::TFreezer Freezer_;
};

////////////////////////////////////////////////////////////////////////////////

class TSimplePidsHolder
    : public IPidsHolder
{
public:
    explicit TSimplePidsHolder(int uid)
        : Uid_(uid)
    { }

    virtual std::vector<int> GetPids() override
    {
        return GetPidsByUid(Uid_);
    }

private:
    const int Uid_;
};

////////////////////////////////////////////////////////////////////////////////

class TContainerPidsHolder
    : public IPidsHolder
{
public:

    virtual std::vector<int> GetPids() override
    {
        auto pids = GetPidsByUid();
        auto my_pid = ::getpid();
        auto it = std::find(pids.begin(), pids.end(), my_pid);
        if (it != pids.end()) {
            pids.erase(it);
        }
        return pids;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJobProbeTools
    : public TRefCounted
{
public:
    ~TJobProbeTools();
    static TJobProbeToolsPtr Create(
        const TJobId& jobId,
        pid_t rootPid,
        int uid,
        const std::vector<TString>& env,
        EJobEnvironmentType environmentType);

    TYsonString StraceJob();
    void SignalJob(const TString& signalName);
    TYsonString PollJobShell(const TYsonString& parameters);
    TFuture<void> AsyncGracefulShutdown(const TError& error);

private:
    const EJobEnvironmentType EnvironmentType_;
    const pid_t RootPid_;
    const int Uid_;
    const std::vector<TString> Environment_;
    const NConcurrency::TActionQueuePtr AuxQueue_;

    std::unique_ptr<IPidsHolder> PidsHolder_;

    std::atomic_flag Stracing_ = ATOMIC_FLAG_INIT;
    IShellManagerPtr ShellManager_;

    TJobProbeTools(pid_t rootPid,
        int uid,
        const std::vector<TString>& env,
        EJobEnvironmentType environmentType);
    void Init(const TJobId& jobId);

    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TJobProbeTools)

////////////////////////////////////////////////////////////////////////////////

TJobProbeTools::TJobProbeTools(
    pid_t rootPid,
    int uid,
    const std::vector<TString>& env,
    EJobEnvironmentType environmentType)
    : EnvironmentType_(environmentType)
    , RootPid_(rootPid)
    , Uid_(uid)
    , Environment_(env)
    , AuxQueue_(New<TActionQueue>("JobAux"))
{ }

TJobProbeToolsPtr TJobProbeTools::Create(
    const TJobId& jobId,
    pid_t rootPid,
    int uid,
    const std::vector<TString>& env,
    EJobEnvironmentType environmentType)
{
    auto tools = New<TJobProbeTools>(rootPid, uid, env, environmentType);
    try {
        tools->Init(jobId);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Unable to create cgroup tools");
        THROW_ERROR_EXCEPTION("Unable to create cgroup tools")
            << ex;
    }
    return tools;
}

void TJobProbeTools::Init(const TJobId& jobId)
{
    switch (EnvironmentType_) {
        case EJobEnvironmentType::Cgroups:
            PidsHolder_.reset(new TFreezerPidsHolder("user_job_" + ToString(jobId)));
            break;

        case EJobEnvironmentType::Porto:
            PidsHolder_.reset(new TContainerPidsHolder());
            break;

        case EJobEnvironmentType::Simple:
            PidsHolder_.reset(new TSimplePidsHolder(Uid_));
            break;

        default:
            Y_UNREACHABLE();
    }

    auto currentWorkDir = NFs::CurrentWorkingDirectory();
    currentWorkDir = currentWorkDir.substr(0, currentWorkDir.find_last_of("/"));
    
    // Copy environment to process arguments
    std::vector<TString> shellEnvironment;
    shellEnvironment.reserve(Environment_.size());
    for (const auto& var : Environment_) {
        if (var.StartsWith("YT_")) {
            shellEnvironment.emplace_back(var);
        }
    }

    ShellManager_ = CreateShellManager(
        NFS::CombinePaths(currentWorkDir, NExecAgent::SandboxDirectoryNames[NExecAgent::ESandboxKind::Home]),
        Uid_,
        EnvironmentType_ == EJobEnvironmentType::Cgroups ? TNullable<TString>("user_job_" + ToString(jobId)) : TNullable<TString>(),
        Format("Job environment:\n%v\n", JoinToString(Environment_, AsStringBuf("\n"))),
        std::move(shellEnvironment));
}

TJobProbeTools::~TJobProbeTools()
{
    if (ShellManager_) {
        BIND(&IShellManager::Terminate, ShellManager_, TError())
            .Via(AuxQueue_->GetInvoker())
            .Run();
    }
}

TYsonString TJobProbeTools::StraceJob()
{
    if (Stracing_.test_and_set()) {
        THROW_ERROR_EXCEPTION("Another strace session is in progress");
    }

    auto guard = Finally([&] () {
        Stracing_.clear();
    });

    auto pids = PidsHolder_->GetPids();

    auto it = std::find(pids.begin(), pids.end(), RootPid_);
    if (it != pids.end()) {
        pids.erase(it);
    }

    LOG_DEBUG("Run strace for %v", pids);

    auto result = WaitFor(BIND([=] () {
        return RunTool<TStraceTool>(pids);
    }).AsyncVia(AuxQueue_->GetInvoker()).Run());

    THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error running job strace tool");

    return ConvertToYsonString(result.Value());
}

void TJobProbeTools::SignalJob(const TString& signalName)
{
    auto arg = New<TSignalerArg>();
    arg->Pids = PidsHolder_->GetPids();

    auto it = std::find(arg->Pids.begin(), arg->Pids.end(), RootPid_);
    if (it != arg->Pids.end()) {
        arg->Pids.erase(it);
    }

    if (arg->Pids.empty()) {
        THROW_ERROR_EXCEPTION("No processes in the job to send signal");
    }

    arg->SignalName = signalName;

    LOG_INFO("Sending signal %v to pids %v",
        arg->SignalName,
        arg->Pids);

    auto result = WaitFor(BIND([=] () {
        return RunTool<TSignalerTool>(arg);
    }).AsyncVia(AuxQueue_->GetInvoker()).Run());

    THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error running job signaler tool");
}

TYsonString TJobProbeTools::PollJobShell(const TYsonString& parameters)
{
    return WaitFor(BIND([=, this_ = MakeStrong(this)] () {
        return ShellManager_->PollJobShell(parameters);
    }).AsyncVia(AuxQueue_->GetInvoker()).Run()).ValueOrThrow();
}

TFuture<void> TJobProbeTools::AsyncGracefulShutdown(const TError& error)
{
    return BIND(&IShellManager::GracefulShutdown, ShellManager_, error)
        .AsyncVia(AuxQueue_->GetInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

class TJobSatelliteWorker
    : public IJobProbe
{
public:
    TJobSatelliteWorker(
        pid_t rootPid,
        int uid,
        const std::vector<TString>& env,
        const TJobId& jobId,
        EJobEnvironmentType environmentType);
    void GracefulShutdown(const TError& error);

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() override;
    virtual TYsonString StraceJob() override;
    virtual void SignalJob(const TString& signalName) override;
    virtual TYsonString PollJobShell(const TYsonString& parameters) override;
    virtual TString GetStderr() override;
    virtual void Interrupt() override;
    virtual void Fail() override;

private:
    const pid_t RootPid_;
    const int Uid_;
    const std::vector<TString> Env_;
    const TJobId JobId_;
    const EJobEnvironmentType EnvironmentType_;

    const NLogging::TLogger Logger;

    TJobProbeToolsPtr JobProbe_;

    void EnsureJobProbe();
};

TJobSatelliteWorker::TJobSatelliteWorker(
    pid_t rootPid,
    int uid,
    const std::vector<TString>& env,
    const TJobId& jobId,
    EJobEnvironmentType environmentType)
    : RootPid_(rootPid)
    , Uid_(uid)
    , Env_(env)
    , JobId_(jobId)
    , EnvironmentType_(environmentType)
    , Logger(NLogging::TLogger(JobSatelliteLogger)
        .AddTag("JobId: %v", JobId_))
{
    YCHECK(JobId_);
    LOG_DEBUG("Starting job satellite service");
}

void TJobSatelliteWorker::EnsureJobProbe()
{
    if (!JobProbe_) {
        JobProbe_ = TJobProbeTools::Create(JobId_, RootPid_, Uid_, Env_, EnvironmentType_);
    }
}

std::vector<TChunkId> TJobSatelliteWorker::DumpInputContext()
{
    Y_UNREACHABLE();
}

TYsonString TJobSatelliteWorker::StraceJob()
{
    EnsureJobProbe();
    return JobProbe_->StraceJob();
}

TString TJobSatelliteWorker::GetStderr()
{
    Y_UNREACHABLE();
}

void TJobSatelliteWorker::SignalJob(const TString& signalName)
{
    EnsureJobProbe();
    JobProbe_->SignalJob(signalName);
}

TYsonString TJobSatelliteWorker::PollJobShell(const TYsonString& parameters)
{
    EnsureJobProbe();
    return JobProbe_->PollJobShell(parameters);
}

void TJobSatelliteWorker::Interrupt()
{
    Y_UNREACHABLE();
}

void TJobSatelliteWorker::Fail()
{
    Y_UNREACHABLE();
}

void TJobSatelliteWorker::GracefulShutdown(const TError &error)
{
    if (JobProbe_) {
        WaitFor(JobProbe_->AsyncGracefulShutdown(error))
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TJobSatellite
    : public TRefCounted
{
public:
    TJobSatellite(
        TJobSatelliteConnectionConfigPtr config,
        pid_t rootPid,
        int uid,
        const std::vector<TString>& env,
        const TJobId& jobId);
    void Run();
    void Stop(const TError& error);

private:
    const TJobSatelliteConnectionConfigPtr SatelliteConnectionConfig_;
    const pid_t RootPid_;
    const int Uid_;
    const std::vector<TString> Env_;
    const TJobId JobId_;
    const NConcurrency::TActionQueuePtr JobSatelliteMainThread_;
    NRpc::IServerPtr RpcServer_;
    IUserJobSynchronizerClientPtr JobProxyControl_;
    TCallback<void(const TError& error)> StopCalback_;
};

TJobSatellite::TJobSatellite(TJobSatelliteConnectionConfigPtr config,
    pid_t rootPid,
    int uid,
    const std::vector<TString>& env,
    const TJobId& jobId)
    : SatelliteConnectionConfig_(config)
    , RootPid_(rootPid)
    , Uid_(uid)
    , Env_(env)
    , JobId_(jobId)
    , JobSatelliteMainThread_(New<TActionQueue>("JobSatelliteMain"))
{ }

void TJobSatellite::Stop(const TError& error)
{
    StopCalback_.Run(error);
    JobProxyControl_->NotifyUserJobFinished(error);
    RpcServer_->Stop().Get();
}

void TJobSatellite::Run()
{
    JobProxyControl_ = CreateUserJobSynchronizerClient(SatelliteConnectionConfig_->JobProxyRpcClientConfig);

    RpcServer_ = NRpc::NBus::CreateBusServer(CreateTcpBusServer(SatelliteConnectionConfig_->SatelliteRpcServerConfig));

    auto jobSatelliteService = New<TJobSatelliteWorker>(
        RootPid_,
        Uid_,
        Env_,
        JobId_,
        SatelliteConnectionConfig_->EnvironmentType
    );

    RpcServer_->RegisterService(CreateJobProberService(jobSatelliteService, JobSatelliteMainThread_->GetInvoker()));
    RpcServer_->Start();

    StopCalback_ = BIND(&TJobSatelliteWorker::GracefulShutdown,
        MakeWeak(jobSatelliteService));

    JobProxyControl_->NotifyJobSatellitePrepared(GetProcessMemoryUsage(-1).Rss);
}

////////////////////////////////////////////////////////////////////////////////

void RunJobSatellite(
    TJobSatelliteConnectionConfigPtr config,
    const int uid,
    const std::vector<TString>& env,
    const TString& jobId)
{
    pid_t pid = fork();
    if (pid == -1) {
        THROW_ERROR_EXCEPTION("Cannot fork")
            << TError::FromSystem();
    } else if (pid == 0) { // child
        return;
    } else {

        NLogging::TLogManager::Get()->Configure(NLogging::TLogConfig::CreateLogFile("../job_satellite.log"));
        try {
            SafeCreateStderrFile("../satellite_stderr");
        } catch (const std::exception& ex) {
            LOG_ERROR("Failed to reopen satellite stderr");
            _exit(1);
        }

        siginfo_t processInfo;
        memset(&processInfo, 0, sizeof(siginfo_t));
        {
            auto jobSatellite = New<TJobSatellite>(config, pid, uid, env, TJobId::FromString(jobId));
            jobSatellite->Run();

            YCHECK(HandleEintr(::waitid, P_PID, pid, &processInfo, WEXITED) == 0);

            jobSatellite->Stop(ProcessInfoToError(processInfo));
        }
        LOG_DEBUG("User process finished (Pid: %v, Status: %v)",
            pid,
            ProcessInfoToError(processInfo));
        NLogging::TLogManager::StaticShutdown();
        _exit(0);
    }
}

void NotifyExecutorPrepared(TJobSatelliteConnectionConfigPtr config)
{
    try {
        auto jobProxyControl = CreateUserJobSynchronizerClient(config->JobProxyRpcClientConfig);
        jobProxyControl->NotifyExecutorPrepared();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error notifying job proxy")
            << ex;
    }
    Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
