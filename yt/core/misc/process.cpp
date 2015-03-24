#include "stdafx.h"

#include "process.h"
#include "proc.h"

#include <core/logging/log.h>

#include <core/misc/error.h>
#include <core/misc/fs.h>

#include <util/system/execpath.h>

#ifndef _win_
  #include <unistd.h>
  #include <errno.h>
  #include <sys/wait.h>
#endif

#ifdef _darwin_
  #include <crt_externs.h>
  #define environ (*_NSGetEnviron())
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Process");

static const pid_t InvalidProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

bool TryKill(int pid, int signal)
{
    YCHECK(pid > 0);
    int result = ::kill(pid, signal);
    if (result < 0) {
        return false;
    }
    return true;
}

bool TryWaitid(idtype_t idtype, id_t id, siginfo_t *infop, int options)
{
    while (true) {
        if (infop != nullptr) {
            // See comment below.
            infop->si_pid = 0;
        }

        auto res = ::waitid(idtype, id, infop, options);

        if (res == 0) {
            // According to man wait.
            // If WNOHANG was specified in options and there were
            // no children in a waitable state, then waitid() returns 0 immediately.
            // To distinguish this case from that where a child
            // was in a waitable state, zero out the si_pid field
            // before the call and check for a nonzero value in this field after
            // the call returns.
            if ((infop != nullptr) && (infop->si_pid == 0)) {
                return false;
            }
            return true;
        }

        if (errno == EINTR) {
            continue;
        }

        return false;
    }
}

void WaitidOrDie(idtype_t idtype, id_t id, siginfo_t *infop, int options)
{
    YCHECK(infop != nullptr);

    memset(infop, 0, sizeof(siginfo_t));

    bool isOK = TryWaitid(idtype, id, infop, options);

    if (!isOK) {
        LOG_FATAL(TError::FromSystem(), "Waitid failed with options: %v", options);
    }

    YCHECK(infop->si_pid == id);
}

void Cleanup(int pid)
{
    YCHECK(pid > 0);

    YCHECK(TryKill(pid, 9));
    YCHECK(TryWaitid(P_PID, pid, nullptr, WEXITED));
}

bool TrySetSignalMask(const sigset_t* sigmask, sigset_t* oldSigmask)
{
    int error = pthread_sigmask(SIG_SETMASK, sigmask, oldSigmask);
    if (error != 0) {
      return false;
    }
    return true;
}

bool TryResetSignals()
{
    for (int sig = 1; sig < NSIG; ++sig) {
        // Ignore invalid signal errors.
        ::signal(sig, SIG_DFL);
    }
    return true;
}

#endif

////////////////////////////////////////////////////////////////////////////////

TProcess::TProcess(const Stroka& path, bool copyEnv)
    : ProcessId_(InvalidProcessId)
    // Stroka is guaranteed to be zero-terminated.
    // https://wiki.yandex-team.ru/Development/Poisk/arcadia/util/StrokaAndTStringBuf#sobstvennosimvoly
    , Path_(path)
{
    AddArgument(NFS::GetFileName(path));

    if (copyEnv) {
        for (char** envIt = environ; *envIt; ++envIt) {
            Env_.push_back(Capture(*envIt));
        }
    }
}

TProcess::~TProcess()
{
    YCHECK(ProcessId_ == InvalidProcessId || Finished_);

    TryClose(Pipe_.ReadFD);
    TryClose(Pipe_.WriteFD);
}

TProcess::TProcess(TProcess&& other)
    : ProcessId_(InvalidProcessId)
{
    Swap(other);
}

void TProcess::Swap(TProcess& other)
{
    std::swap(Started_, other.Started_);
    std::swap(Finished_, other.Finished_);
    std::swap(ProcessId_, other.ProcessId_);
    std::swap(Path_, other.Path_);
    std::swap(MaxSpawnActionFD_, other.MaxSpawnActionFD_);
    // All iterators, pointers and references referring to elements
    // in both containers remain valid, and are now referring to
    // the same elements they referred to before
    // the call, but in the other container, where they now iterate.
    // Note that the end iterators do not refer to elements and may be invalidated.
    StringHolder_.swap(other.StringHolder_);
    Args_.swap(other.Args_);
    Env_.swap(other.Env_);
    SpawnActions_.swap(other.SpawnActions_);
}


TProcess TProcess::CreateCurrentProcessSpawner()
{
    return TProcess(GetExecPath(), true);
}

void TProcess::AddArgument(TStringBuf arg)
{
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    Args_.push_back(Capture(arg));
}

void TProcess::AddEnvVar(TStringBuf var)
{
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    Env_.push_back(Capture(var));
}

void TProcess::AddArguments(std::initializer_list<TStringBuf> args)
{
    for (auto arg : args) {
        AddArgument(arg);
    }
}

void TProcess::AddCloseFileAction(int fd)
{
    TSpawnAction action = {
        std::bind(TryClose, fd),
        Format("Error closing %v file descriptor in the child", fd)
    };

    MaxSpawnActionFD_ = std::max(MaxSpawnActionFD_, fd);
    SpawnActions_.push_back(action);
}

void TProcess::AddDup2FileAction(int oldFD, int newFD)
{
    TSpawnAction action = {
        std::bind(TryDup2, oldFD, newFD),
        Format("Error duplicating %v file descriptor to %v in the child", oldFD, newFD)
    };

    MaxSpawnActionFD_ = std::max(MaxSpawnActionFD_, newFD);
    SpawnActions_.push_back(action);
}

void TProcess::Spawn()
{
#ifdef _linux_
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    // Make sure no spawn action closes Pipe_.WriteFD
    TPipeFactory pipeFactory(MaxSpawnActionFD_ + 1);
    Pipe_ = pipeFactory.Create();
    pipeFactory.Clear();

    LOG_DEBUG("Process arguments: [%v], environment: [%v]", JoinToString(Args_), JoinToString(Env_));

    Env_.push_back(nullptr);
    Args_.push_back(nullptr);

    // Block all signals around vfork; see http://ewontfix.com/7/

    // As the child may run in the same address space as the parent until
    // the actual execve() system call, any (custom) signal handlers that
    // the parent has might alter parent's memory if invoked in the child,
    // with undefined results.  So we block all signals in the parent before
    // vfork(), which will cause them to be blocked in the child as well (we
    // rely on the fact that Linux, just like all sane implementations, only
    // clones the calling thread).  Then, in the child, we reset all signals
    // to their default dispositions (while still blocked), and unblock them
    // (so the exec()ed process inherits the parent's signal mask)

    sigset_t allBlocked;
    sigfillset(&allBlocked);
    sigset_t oldSignals;

    if (!TrySetSignalMask(&allBlocked, &oldSignals)) {
        THROW_ERROR_EXCEPTION("Failed to block all signals")
            << TError::FromSystem();
    }

    SpawnActions_.push_back(TSpawnAction {
        TryResetSignals,
        "Error resetting signals to default disposition in the child: signal failed"
    });

    SpawnActions_.push_back(TSpawnAction {
        std::bind(TrySetSignalMask, &oldSignals, nullptr),
        "Error unblocking signals in the child: pthread_sigmask failed"
    });

    SpawnActions_.push_back(TSpawnAction {
        std::bind(TryExecve, ~Path_, Args_.data(), Env_.data()),
        "Error starting child process: execve failed"
    });

    SpawnChild();

    LOG_DEBUG("Child process is spawned. Pid: %v", ProcessId_);

    // This should not fail ever.
    YCHECK(TrySetSignalMask(&oldSignals, nullptr));

    YCHECK(TryClose(Pipe_.WriteFD));
    Pipe_.WriteFD = TPipe::InvalidFD;

    ThrowOnChildError();
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TProcess::SpawnChild()
{
#ifdef _linux_
    int pid = vfork();

    if (pid < 0) {
        THROW_ERROR_EXCEPTION("Error starting child process: vfork failed")
            << TErrorAttribute("path", Path_)
            << TError::FromSystem();
    }

    if (pid == 0) {
        Child();
    }

    ProcessId_ = pid;

    {
        TGuard<TSpinLock> guard(LifecycleChangeLock_);
        Started_ = true;
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TProcess::ThrowOnChildError()
{
#ifdef _linux_
    int data[2];
    int res = ::read(Pipe_.ReadFD, &data, sizeof(data));
    if (res == 0) {
        // Child successfully spawned or was killed by a signal.
        // But there is no way to ditinguish between two situations:
        // * child killed by signal before exec
        // * child killed by signal after exec
        // So we treat kill-before-exec the same way as kill-after-exec
        LOG_DEBUG("Command execed. Pid: %v", ProcessId_);
        return;
    }

    YCHECK(res == sizeof(data));

    {
        TGuard<TSpinLock> guard(LifecycleChangeLock_);
        Finished_ = true;
    }

#ifdef _linux_
    Cleanup(ProcessId_);
#endif
    ProcessId_ = InvalidProcessId;

    int actionIndex = data[0];
    int errorCode = data[1];

    YCHECK(0 <= actionIndex && actionIndex < SpawnActions_.size());
    const auto& action = SpawnActions_[actionIndex];
    THROW_ERROR_EXCEPTION("%v", action.ErrorMessage)
        << TError::FromSystem(errorCode);
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

#ifdef _linux_
TError ProcessInfoToError(const siginfo_t& processInfo)
{
    int signalBase = static_cast<int>(EExitStatus::SignalBase);
    if (processInfo.si_code == CLD_EXITED) {
        auto exitCode = processInfo.si_status;
        if (exitCode == 0) {
            return TError();
        } else {
            return TError(
                signalBase + exitCode,
                "Process exited with code %v",
                exitCode);
        }
    } else if ((processInfo.si_code == CLD_KILLED) || (processInfo.si_code == CLD_DUMPED)) {
        return TError(
            signalBase + processInfo.si_status,
            "Process terminated by signal %v",
            processInfo.si_status);
    }
    YUNREACHABLE();
}
#endif

TError TProcess::Wait()
{
#ifdef _linux_
    YCHECK(ProcessId_ != InvalidProcessId);
    LOG_DEBUG("Start to wait for %v to finish", ProcessId_);

    siginfo_t processInfo;

    // Note WNOWAIT flag.
    // This call just waits for a process to be finished but does not clear zombie flag.
    WaitidOrDie(P_PID, ProcessId_, &processInfo, WEXITED | WNOWAIT);

    {
        TGuard<TSpinLock> guard(LifecycleChangeLock_);

        // This call just should return immediately
        // because we have already waited for this process with WNOHANG
        WaitidOrDie(P_PID, ProcessId_, &processInfo, WEXITED | WNOHANG);

        Finished_ = true;
    }
    LOG_DEBUG("Finish to wait for %v to finish", ProcessId_);

    return ProcessInfoToError(processInfo);
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TProcess::Kill(int signal)
{
#ifdef _linux_
    LOG_DEBUG("Kill %v process", ProcessId_);

    TGuard<TSpinLock> guard(LifecycleChangeLock_);

    if (!Started_) {
        THROW_ERROR_EXCEPTION("Process is not started yet");
    }

    if (Finished_) {
        return;
    }

    auto result = TryKill(ProcessId_, signal);
    if (!result) {
        THROW_ERROR_EXCEPTION("kill failed")
            << TError::FromSystem();
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

int TProcess::GetProcessId() const
{
    return ProcessId_;
}

bool TProcess::Started() const
{
    return Started_;
}

bool TProcess::Finished() const
{
    return Finished_;
}

char* TProcess::Capture(TStringBuf arg)
{
    StringHolder_.push_back(Stroka(arg));
    return const_cast<char*>(~StringHolder_.back());
}

void TProcess::Child()
{
#ifdef _linux_
    YCHECK(Pipe_.WriteFD != TPipe::InvalidFD);

    for (int actionIndex = 0; actionIndex < SpawnActions_.size(); ++actionIndex) {
        auto& action = SpawnActions_[actionIndex];
        if (!action.Callback()) {
            // Report error through the pipe.
            int data[] = {
                actionIndex,
                errno
            };

            // According to pipe(7) write of small buffer is atomic.
            YCHECK(::write(Pipe_.WriteFD, &data, sizeof(data)) == sizeof(data));
            _exit(1);
        }
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
