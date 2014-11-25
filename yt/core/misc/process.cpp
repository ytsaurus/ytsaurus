#include "stdafx.h"

#include "process.h"
#include "proc.h"

#include <core/logging/log.h>
#include <core/misc/error.h>
#include <core/misc/fs.h>

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

static NLog::TLogger Logger("Process");

static const pid_t InvalidProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

void SafeAtomicCloseExecPipe(int pipefd[2])
{
#if defined(_linux_)
    auto result = ::pipe2(pipefd, O_CLOEXEC);
    if (result == -1) {
        THROW_ERROR_EXCEPTION("Error creating pipe") << TError::FromSystem();
    }
#elif defined(_darwin_)
    {
        int result = ::pipe(pipefd);
        if (result == -1) {
            THROW_ERROR_EXCEPTION("Error creating pipe: pipe creation failed") << TError::FromSystem();
        }
    }
    for (int index = 0; index < 2; ++index) {
        int getResult = ::fcntl(pipefd[index], F_GETFL);
        if (getResult == -1) {
            THROW_ERROR_EXCEPTION("Error creating pipe: fcntl failed to get descriptor flags") << TError::FromSystem();
        }

        int setResult = ::fcntl(pipefd[index], F_SETFL, getResult | FD_CLOEXEC);
        if (setResult == -1) {
            THROW_ERROR_EXCEPTION("Error creating pipe: fcntl failed to set descriptor flags") << TError::FromSystem();
        }
    }
#else
    THROW_ERROR_EXCEPTION("Windows is not supported");
#endif
}

////////////////////////////////////////////////////////////////////////////////

TProcess::TProcess(const Stroka& path, bool copyEnv)
    : Finished_(false)
    , Status_(0)
    , ProcessId_(InvalidProcessId)
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
    if (ProcessId_ != InvalidProcessId) {
        YCHECK(Finished_);
    }

    if (Pipe_.ReadFd != TPipe::InvalidFd) {
        ::close(Pipe_.ReadFd);
        Pipe_.ReadFd = TPipe::InvalidFd;
    }

    if (Pipe_.WriteFd != TPipe::InvalidFd) {
        ::close(Pipe_.WriteFd);
        Pipe_.WriteFd = TPipe::InvalidFd;
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

void TProcess::AddCloseFileAction(int fd)
{
    TSpawnAction action = {
        std::bind(TryClose, fd),
        Format("Error closing %v file descriptor in the child", fd)
    };

    SpawnActions_.push_back(action);
}

void TProcess::AddDup2FileAction(int oldFd, int newFd)
{
    TSpawnAction action = {
        std::bind(TryDup2, oldFd, newFd),
        Format("Error duplicating %v file descriptor to %v in the child", oldFd, newFd)
    };

    SpawnActions_.push_back(action);
}

void TProcess::Spawn()
{
#ifdef _win_
    THROW_ERROR_EXCEPTION("Windows is not supported");
#else
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    int pipe[2];
    SafeAtomicCloseExecPipe(pipe);
    Pipe_ = TPipe(pipe);

    LOG_DEBUG("Process arguments: %v", JoinToString(Args_));
    LOG_DEBUG("Process environment: %v", JoinToString(Env_));

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

    YCHECK(::close(Pipe_.WriteFd) == 0);
    Pipe_.WriteFd = TPipe::InvalidFd;

    int data[2];
    int res = ::read(Pipe_.ReadFd, &data, sizeof(data));
    if (res == 0) {
        // Child successfully spawned.
        ProcessId_ = pid;
        return;
    }

    YCHECK(res == sizeof(data));

    ::waitpid(pid, nullptr, 0);
    Finished_ = true;

    int actionIndex = data[0];
    int errorCode = data[1];

    YCHECK(0 <= actionIndex && actionIndex < SpawnActions_.size());
    const auto& action = SpawnActions_[actionIndex];
    THROW_ERROR_EXCEPTION("%v", action.ErrorMessage) << TError::FromSystem(errorCode);
#endif
}

TError TProcess::Wait()
{
#ifdef _win_
    return TError("Windows is not supported");
#else

    int result = ::waitpid(ProcessId_, &Status_, WUNTRACED);
    Finished_ = true;

    if (result < 0) {
        return TError("waitpid failed") << TError::FromSystem();
    }

    YCHECK(result == ProcessId_);
    return StatusToError(Status_);
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
    YCHECK(Pipe_.WriteFd != TPipe::InvalidFd);

    for (int actionIndex = 0; actionIndex < SpawnActions_.size(); ++actionIndex) {
        auto& action = SpawnActions_[actionIndex];
        if (!action.Callback()) {
            // Report error through the pipe.
            int data[] = {
                actionIndex,
                errno
            };

            // According to pipe(7) write of small buffer is atomic.
            YCHECK(::write(Pipe_.WriteFd, &data, sizeof(data)) == sizeof(data));
            _exit(1);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
