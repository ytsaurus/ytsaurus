#pragma once

#include <util/stream/output.h>
#include <util/generic/noncopyable.h>
#include <util/generic/maybe.h>

#include <functional>
#include <unordered_map>
#include <list>
#include <utility>

#include <sys/wait.h>


namespace NYql {

//////////////////////////////////////////////////////////////////////////////
// TProcessStatus
//////////////////////////////////////////////////////////////////////////////
class TProcessStatus
{
public:
    explicit TProcessStatus(int status = -1)
        : Status_(status)
    {
    }

    bool IsExited() const {
        return WIFEXITED(Status_);
    }

    int GetExitStatus() const {
        return WEXITSTATUS(Status_);
    }

    bool IsTerminated() const {
        return WIFSIGNALED(Status_);
    }

    int GetTerminationSignal() const {
        return WTERMSIG(Status_);
    }

    TProcessStatus ReplaceTerminationSignal(int newSignal) const {
        int newStatus = Status_ - GetTerminationSignal() + newSignal;
        return TProcessStatus(newStatus);
    }

    bool IsCoreDumped() const {
        return WCOREDUMP(Status_);
    }

    bool IsStopped() const {
        return WIFSTOPPED(Status_);
    }

    int GetStopSignal() const {
        return WSTOPSIG(Status_);
    }

    bool IsContinued() const {
        return WIFCONTINUED(Status_);
    }

    int GetRawStatus() const {
        return Status_;
    }

private:
    int Status_;
};

using TProcessMainFunction = std::function<int(void*)>;

//////////////////////////////////////////////////////////////////////////////
// TProcess
//////////////////////////////////////////////////////////////////////////////
class TProcess: private TMoveOnly
{
public:
    explicit TProcess(TProcessMainFunction mainFn)
        : Pid_(-1)
        , MainFn_(std::move(mainFn))
    {
    }

    ~TProcess();

    TProcess(TProcess&& other)
        : Pid_(other.Pid_)
        , Status_(other.Status_)
        , MainFn_(std::move(other.MainFn_))
    {
        other.Pid_ = -1;
        other.Status_ = TProcessStatus();
    }

    pid_t GetId() const { return Pid_; }
    TProcessStatus GetStatus() const { return Status_; }

    pid_t Spawn(void* param = nullptr);
    TProcessStatus Wait() const;
    void Detach();
    void SendSignal(int sig) const;

    bool IsSpawned() const { return Pid_ > 0; }
    bool IsDetached() const { return Pid_ == 0; }
    bool IsFinished() const;

private:
    pid_t Pid_;
    mutable TProcessStatus Status_;
    TProcessMainFunction MainFn_;
};

//////////////////////////////////////////////////////////////////////////////
// TProcessPool
//////////////////////////////////////////////////////////////////////////////
// XXX: Not thread-safe, must be used only in main thread
class TProcessPool: private TMoveOnly
{
    enum class EProcessState {
        SPAWNED,
        TERMINATING,
    };

    struct TProcessWithState: private TMoveOnly {
        TProcess P;
        EProcessState S;

        TProcessWithState(TProcessMainFunction fn, EProcessState state)
            : P(std::move(fn))
            , S(state)
        {
        }

        TProcessWithState(TProcessWithState&&) = default;
    };

    struct TProcessInfo: private TMoveOnly {
        TString Name;
        ui32 Count;
        TProcessMainFunction MainFn;

        TProcessInfo(TStringBuf name, ui32 count, TProcessMainFunction fn)
            : Name(name)
            , Count(count)
            , MainFn(std::move(fn))
        {
        }
    };

public:
    struct TExitStatus {
        pid_t Pid = 0;
        int ExitCode = -1;
        ui32 Type = 0;
    };

    ~TProcessPool();

    void Add(ui32 type, TStringBuf name, ui32 count, TProcessMainFunction fn);
    void SpawnChildren(void* param = nullptr);
    void SpawnOneMore(ui32 type, void* param = nullptr);
    void ReapZombies(TVector<TExitStatus>* statuses = nullptr);
    void Shutdown(int signal, TMaybe<ui32> type = {}, bool force = false);
    void Wait();
    void Detach();
    void SendSignal(pid_t pid, int signo);
    bool Empty() const;

private:
    const TProcessInfo& GetProcessInfo(ui32 type);


private:
    std::unordered_map<ui32, TProcessInfo> ProcessInfos_;
    std::unordered_map<ui32, std::list<TProcessWithState>> ProcessesByType_;
    std::unordered_map<pid_t, TProcessWithState*> ProcessesByPid_;
};

} // namespace NYql


template <>
void Out<NYql::TProcessStatus>(IOutputStream& out, const NYql::TProcessStatus& s);
