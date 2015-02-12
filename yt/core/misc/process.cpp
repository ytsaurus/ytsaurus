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

static NLogging::TLogger Logger("Process");

static const pid_t InvalidProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

TProcess::TProcess(const Stroka& path, bool copyEnv)
    : Finished_(false)
    , Status_(0)
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

    LOG_DEBUG("Process arguments: %v, environment: %v", JoinToString(Args_), JoinToString(Env_));

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

    YCHECK(::close(Pipe_.WriteFD) == 0);
    Pipe_.WriteFD = TPipe::InvalidFd;

    int data[2];
    int res = ::read(Pipe_.ReadFD, &data, sizeof(data));
    if (res == 0) {
        // Child successfully spawned.
        ProcessId_ = pid;
        return;
    }

    YCHECK(res == sizeof(data));
    YCHECK(::waitpid(pid, nullptr, 0) == pid);

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
