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

#endif

////////////////////////////////////////////////////////////////////////////////

TProcess::TProcess()
    // TODO(tramsmm) Should I really copy the environment?
    : TProcess(GetExecPath(), true)
{ }

TProcess::TProcess(const Stroka& path, bool copyEnv)
    : Started_(false)
    , Finished_(false)
    , ProcessId_(InvalidProcessId)
    // Stroka is guaranteed to be zero-terminated.
    // https://wiki.yandex-team.ru/Development/Poisk/arcadia/util/StrokaAndTStringBuf#sobstvennosimvoly
    , Path_(path)
    , MaxSpawnActionFD_(-1)
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
    if (ProcessId_ != InvalidProcessId) {
        YCHECK(Finished_);
    }

    if (Pipe_.ReadFD != TPipe::InvalidFd) {
        ::close(Pipe_.ReadFD);
        Pipe_.ReadFD = TPipe::InvalidFd;
    }

    if (Pipe_.WriteFD != TPipe::InvalidFd) {
        ::close(Pipe_.WriteFD);
        Pipe_.WriteFD = TPipe::InvalidFd;
    }
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

void TProcess::AddDup2FileAction(int oldFd, int newFd)
{
    TSpawnAction action = {
        std::bind(TryDup2, oldFd, newFd),
        Format("Error duplicating %v file descriptor to %v in the child", oldFd, newFd)
    };

    MaxSpawnActionFD_ = std::max(MaxSpawnActionFD_, newFd);
    SpawnActions_.push_back(action);
}

void TProcess::Spawn()
{
#ifdef _win_
    THROW_ERROR_EXCEPTION("Windows is not supported");
#else
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    // Make sure no spawn action closes Pipe_.WriteFD
    TPipeFactory pipeFactory(MaxSpawnActionFD_ + 1);
    Pipe_ = pipeFactory.Create();
    pipeFactory.Clear();

    LOG_DEBUG("Process arguments: [%v], environment: [%v]", JoinToString(Args_), JoinToString(Env_));

    Env_.push_back(nullptr);
    Args_.push_back(nullptr);

    bool useVFork = SpawnActions_.empty();

    SpawnActions_.push_back(TSpawnAction {
        std::bind(TryExecve, ~Path_, Args_.data(), Env_.data()),
        "Error starting child process: execve failed"
    });

    int pid = -1;
    if (useVFork) {
        // One is not allowed to call close and dup2 after vfork.
        pid = vfork();
    } else {
        pid = fork();
    }

    if (pid == 0) {
        DoSpawn();
    }

    if (pid < 0) {
        THROW_ERROR_EXCEPTION("Error starting child process: %v failed", useVFork ? "vfork" : "fork")
            << TErrorAttribute("path", Path_)
            << TError::FromSystem();
    }

    ProcessId_ = pid;

    {
        TGuard<TSpinLock> guard(LifecycleChangeLock_);
        Started_ = true;
    }

    LOG_DEBUG("Children process is spawned. Pid: %v", ProcessId_);

    YCHECK(::close(Pipe_.WriteFD) == 0);
    Pipe_.WriteFD = TPipe::InvalidFd;

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

    ProcessId_ = InvalidProcessId;

    YCHECK(res == sizeof(data));
    YCHECK(TryWaitid(P_PID, pid, nullptr, WEXITED));

    {
        TGuard<TSpinLock> guard(LifecycleChangeLock_);
        Finished_ = true;
    }

    int actionIndex = data[0];
    int errorCode = data[1];

    YCHECK(0 <= actionIndex && actionIndex < SpawnActions_.size());
    const auto& action = SpawnActions_[actionIndex];
    THROW_ERROR_EXCEPTION("%v", action.ErrorMessage) << TError::FromSystem(errorCode);
#endif
}

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

TError TProcess::Wait()
{
#ifdef _win_
    return TError("Windows is not supported");
#else
    YCHECK(ProcessId_ != InvalidProcessId);
    LOG_DEBUG("Start to wait for %v to finish", ProcessId_);

    siginfo_t processInfo;
    memset(&processInfo, 0, sizeof(processInfo));

    // Note WNOWAIT flag.
    // This call just waits for a process to be finished but does not clear zombie flag.
    bool isOK = TryWaitid(P_PID, ProcessId_, &processInfo, WEXITED | WNOWAIT);

    if (!isOK) {
        return TError("WNOWAIT waitid failed") << TError::FromSystem();
    }
    YCHECK(processInfo.si_pid == ProcessId_);

    memset(&processInfo, 0, sizeof(processInfo));
    {
        TGuard<TSpinLock> guard(LifecycleChangeLock_);

        // This call just should return immediately
        // because we have already waited for this process with WNOHANG
        isOK = TryWaitid(P_PID, ProcessId_, &processInfo, WEXITED | WNOHANG);

        if (!isOK) {
            return TError("waitid failed") << TError::FromSystem();
        }
        YCHECK(processInfo.si_pid == ProcessId_);

        Finished_ = true;
    }
    LOG_DEBUG("Finish to wait for %v to finish", ProcessId_);

    return ProcessInfoToError(processInfo);
#endif
}

void TProcess::Kill(int signal)
{
#ifdef _win_
    THROW_ERROR_EXCEPTION("Windows is not supported");
#else

    LOG_DEBUG("Kill %v process", ProcessId_);

    TGuard<TSpinLock> guard(LifecycleChangeLock_);

    if (!Started_) {
        THROW_ERROR_EXCEPTION("Process is not started yet");
    }

    if (Finished_) {
        return;
    }

    int result = ::kill(ProcessId_, signal);
    if (result < 0) {
        THROW_ERROR_EXCEPTION("kill failed") << TError::FromSystem();
    }
#endif
}

int TProcess::GetProcessId() const
{
    return ProcessId_;
}

char* TProcess::Capture(TStringBuf arg)
{
    StringHolder_.push_back(Stroka(arg));
    return const_cast<char*>(~StringHolder_.back());
}

void TProcess::DoSpawn()
{
    YCHECK(Pipe_.WriteFD != TPipe::InvalidFd);

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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
