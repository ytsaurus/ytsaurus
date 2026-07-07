#include "multiprocessing.h"

#include <yql/essentials/utils/log/log.h>

#include <util/generic/yexception.h>

#include <stdlib.h>
#include <errno.h>

#define NO_EINTR(stmt) while ((stmt) == -1 && errno == EINTR);

namespace NYql {

//////////////////////////////////////////////////////////////////////////////
// TProcess
//////////////////////////////////////////////////////////////////////////////
TProcess::~TProcess()
{
    if (IsSpawned() && !IsFinished()) {
        SendSignal(SIGKILL);
        Wait();
    }
}

pid_t TProcess::Spawn(void* param)
{
    switch (Pid_ = fork()) {
    case -1:
        ythrow TSystemError() << "cannot fork subprocess";

    case 0: {
        // child
        int status = EXIT_FAILURE;
        try {
            status = MainFn_(param);
        } catch (...) {
            status = EXIT_FAILURE;
            TString message = CurrentExceptionMessage();
            if (NLog::IsYqlLoggerInitialized()) {
                YQL_LOG(FATAL) << "exception from process main(): " << message;
            } else {
                Cerr << "pid: " << getpid() << ", exception from process main(): " << message << Endl;
            }
        }
        NLog::CleanupLogger();
        _exit(status);
    }

    default: // parent
        MainFn_ = nullptr; // TProcess is not reusable
        break;
    }

    return Pid_;
}

bool TProcess::IsFinished() const
{
    if (Status_.IsExited() || Status_.IsTerminated()) {
        return true;
    }

    Y_ENSURE(Pid_ > 0, "process not spawned or detached");

    int status;
    pid_t p;
    NO_EINTR(p = waitpid(Pid_, &status, WNOHANG));
    if (p == 0) {
        // child specified by Pid_ exist, but have not yet changed the state
        return false;
    }

    Y_ENSURE_EX(p != -1,
            TSystemError() << "cannot wait process with pid " << Pid_);

    Status_ = TProcessStatus(status);
    return Status_.IsExited() || Status_.IsTerminated();
}

TProcessStatus TProcess::Wait() const
{
    if (Status_.IsExited() || Status_.IsTerminated()) {
        return Status_;
    }

    Y_ENSURE(Pid_ > 0, "process not spawned or detached");

    int status;
    pid_t p;
    NO_EINTR(p = waitpid(Pid_, &status, 0));

    Y_ENSURE_EX(p != -1,
            TSystemError() << "cannot wait process with pid " << Pid_);

    return Status_ = TProcessStatus(status);
}

void TProcess::Detach()
{
    Y_ENSURE(Pid_ > 0, "process not spawned or detached");
    Pid_ = 0;
}

void TProcess::SendSignal(int sig) const
{
    if (Status_.IsExited() || Status_.IsTerminated()) {
        return;
    }

    Y_ENSURE(Pid_ > 0, "process not spawned or detached");

    int rc = kill(Pid_, sig);
    Y_ENSURE(rc == 0 || errno == ESRCH,
             "cannot send signal " << sig << " to process " << Pid_);
}

//////////////////////////////////////////////////////////////////////////////
// TProcessPool
//////////////////////////////////////////////////////////////////////////////
TProcessPool::~TProcessPool()
{
    Shutdown(SIGKILL);
    Wait();
}

void TProcessPool::Add(
        ui32 type, TStringBuf name, ui32 count, TProcessMainFunction fn)
{
    auto res = ProcessInfos_.emplace(type, TProcessInfo(name, count, std::move(fn)));
    Y_ENSURE(res.second, "process with type " << type << " already exists");
}

void TProcessPool::SpawnChildren(void* param)
{
    for (const auto& it: ProcessInfos_) {
        ui32 type = it.first;
        const TProcessInfo& procInfo = it.second;
        std::list<TProcessWithState>& children = ProcessesByType_[type];

        i32 spawnedCount = 0;
        for (TProcessWithState& child: children) {
            if (child.S == EProcessState::SPAWNED) {
                spawnedCount++;
            }
        }

        i32 toSpawn = (i32) procInfo.Count - spawnedCount;
        while (toSpawn-- > 0) {
            children.emplace_back(procInfo.MainFn, EProcessState::SPAWNED);
            TProcessWithState& child = children.back();
            pid_t childPid = child.P.Spawn(param);
            ProcessesByPid_.emplace(childPid, &child);

            YQL_LOG(INFO) << "spawned child process " << procInfo.Name
                          << " (pid=" << childPid << ')';
        }
    }
}

void TProcessPool::SpawnOneMore(ui32 type, void* param)
{
    auto it = ProcessInfos_.find(type);
    if (it == ProcessInfos_.end()) {
        ythrow yexception() << "not registered process type: " << type;
    }

    const TProcessInfo& procInfo = it->second;
    TProcessWithState child(procInfo.MainFn, EProcessState::SPAWNED);
    // Spawn can throw
    const pid_t childPid = child.P.Spawn(param);
    std::list<TProcessWithState>& children = ProcessesByType_[type];
    children.emplace_back(std::move(child));
    ProcessesByPid_.emplace(childPid, &children.back());

    YQL_LOG(INFO) << "spawned child process " << procInfo.Name
        << " (pid=" << childPid << ')';
}

void TProcessPool::ReapZombies(TVector<TExitStatus>* statuses)
{
    for (auto& mapIt: ProcessesByType_) {
        ui32 type = mapIt.first;
        std::list<TProcessWithState>& children = mapIt.second;

        for (auto listIt = children.begin(),
                 listEnd = children.end(); listIt != listEnd; )
        {
            TProcess& child = listIt->P;
            if (child.IsFinished()) {
                const TProcessInfo& procInfo = GetProcessInfo(type);
                YQL_LOG(INFO) << "child process " << procInfo.Name
                              <<" (pid=" << child.GetId() << "): "
                             << child.GetStatus();
                ProcessesByPid_.erase(child.GetId());
                if (statuses) {
                    TExitStatus status;
                    status.Pid = child.GetId();
                    status.ExitCode = child.GetStatus().GetRawStatus();
                    status.Type = type;
                    statuses->push_back(status);
                }

                listIt = children.erase(listIt);
            } else {
                ++listIt;
            }
        }
    }
}

void TProcessPool::Shutdown(int signal, TMaybe<ui32> typeToShutdown, bool force)
{
    for (auto& mapIt: ProcessesByType_) {
        ui32 type = mapIt.first;
        if (typeToShutdown && *typeToShutdown != type) {
            continue;
        }
        std::list<TProcessWithState>& children = mapIt.second;

        for (TProcessWithState& child: children) {
            if (force || child.S == EProcessState::SPAWNED) {
                const TProcessInfo& procInfo = GetProcessInfo(type);
                YQL_LOG(INFO) << "send signal " << signal << " to child process "
                              << procInfo.Name << " (pid=" << child.P.GetId() << ')';
                child.S = EProcessState::TERMINATING;
                child.P.SendSignal(signal);
            }
        }
    }
}

void TProcessPool::Wait()
{
    for (auto& mapIt: ProcessesByType_) {
        ui32 type = mapIt.first;
        std::list<TProcessWithState>& children = mapIt.second;

        for (auto listIt = children.begin(),
                 listEnd = children.end(); listIt != listEnd; )
        {
            const TProcess& child = listIt->P;
            if (child.IsSpawned() && !child.IsFinished()) {
                child.Wait();
            }

            const TProcessInfo& procInfo = GetProcessInfo(type);
            YQL_LOG(INFO) << "child process " << procInfo.Name
                          << " (pid=" << child.GetId() << "): "
                          << child.GetStatus();
            ProcessesByPid_.erase(child.GetId());
            listIt = children.erase(listIt);
        }
    }
}

void TProcessPool::Detach()
{
    for (const auto& mapIt: ProcessesByPid_) {
        TProcessWithState* child = mapIt.second;
        child->P.Detach();
    }
    ProcessesByPid_.clear();
    ProcessesByType_.clear();
}

void TProcessPool::SendSignal(pid_t pid, int signo)
{
    auto it = ProcessesByPid_.find(pid);
    if (it == ProcessesByPid_.end()) {
        ythrow yexception() << "unknown process: " << pid;
    }

    TProcessWithState* child = it->second;
    child->P.SendSignal(signo);
}

bool TProcessPool::Empty() const
{
    return ProcessesByPid_.empty();
}

const TProcessPool::TProcessInfo& TProcessPool::GetProcessInfo(ui32 type)
{
    auto it = ProcessInfos_.find(type);
    Y_ENSURE(it != ProcessInfos_.end(), "unknown process type: " << type);
    return it->second;
}

} // namespace NYql

template <>
void Out<NYql::TProcessStatus>(IOutputStream& out, const NYql::TProcessStatus& s)
{
    if (s.IsExited()) {
        out << TStringBuf("exited with status=") << s.GetExitStatus();
    } else if (s.IsTerminated()) {
        out << TStringBuf("terminated by signal=") << s.GetTerminationSignal();
        if (s.IsCoreDumped()) {
            out << TStringBuf(" (core dumped)");
        }
    } else if (s.IsStopped()) {
        out << TStringBuf("stopped by signal=") << s.GetStopSignal();
    } else if (s.IsContinued()) {
        out << TStringBuf("continued");
    } else {
        out << TStringBuf("invalid process status=")  << s.GetRawStatus();
    }
}
