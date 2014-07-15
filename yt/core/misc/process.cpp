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

static const int InvalidFd = -1;
static const pid_t InvalidProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

TError SafeAtomicCloseExecPipe(int pipefd[2])
{
#if defined(_linux_)
    auto result = pipe2(pipefd, O_CLOEXEC);
    if (result == -1) {
        return TError("Error creating pipe")
            << TError::FromSystem();
    }
    return TError();
#elif defined(_darwin_)
    {
        int result = pipe(pipefd);
        if (result == -1) {
            return TError("Error creating pipe: pipe creation failed")
                << TError::FromSystem();
        }
    }
    for (int index = 0; index < 2; ++index) {
        int getResult = ::fcntl(pipefd[index], F_GETFL);
        if (getResult == -1) {
            return TError("Error creating pipe: fcntl failed to get descriptor flags")
                << TError::FromSystem();
        }

        int setResult = ::fcntl(pipefd[index], F_SETFL, getResult | FD_CLOEXEC);
        if (setResult == -1) {
            return TError("Error creating pipe: fcntl failed to set descriptor flags")
                << TError::FromSystem();
        }
    }
    return TError();
#else
    return TError("Windows is not supported");
#endif
}

TProcess::TProcess(const Stroka& path, bool copyEnv)
    : Finished_(false)
    , Status_(0)
    , ProcessId_(InvalidProcessId)
{
    Path_.insert(Path_.end(), path.begin(), path.end());
    Path_.push_back(0);

    AddArgument(NFS::GetFileName(path));

    if (copyEnv) {
        char** envIt = environ;
        while (*envIt) {
            const char* const item = *envIt;
            Env_.push_back(Capture(TStringBuf(item)));
            ++envIt;
        }
    }
}

TProcess::~TProcess()
{
    if (ProcessId_ != InvalidProcessId) {
        YCHECK(Finished_);
    }

    if (Pipe_.ReadFd != InvalidFd) {
        ::close(Pipe_.ReadFd);
        Pipe_.ReadFd = InvalidFd;
    }

    if (Pipe_.WriteFd != InvalidFd) {
        ::close(Pipe_.WriteFd);
        Pipe_.WriteFd = InvalidFd;
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

TError TProcess::Spawn()
{
#ifdef _win_
    return TError("Windows is not supported");
#else
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    int pipe[2];
    auto error = SafeAtomicCloseExecPipe(pipe);
    if (!error.IsOK()) {
        return error;
    }
    Pipe_ = TPipe(pipe);

    LOG_DEBUG("Process arguments: %v", JoinToString(Args_));
    LOG_DEBUG("Process environment: %v", JoinToString(Env_));

    Env_.push_back(nullptr);
    Args_.push_back(nullptr);

    ChildPipe_ = Pipe_;

    int pid = vfork();
    if (pid == 0) {
        DoSpawn();
    }

    if (pid < 0) {
        return TError("Error starting child process: clone failed")
            << TErrorAttribute("path", GetPath())
            << TError::FromSystem();
    }

    YCHECK(::close(Pipe_.WriteFd) == 0);
    Pipe_.WriteFd = InvalidFd;

    {
        int errCode;
        if (::read(Pipe_.ReadFd, &errCode, sizeof(int)) == sizeof(int)) {
            ::waitpid(pid, nullptr, 0);
            Finished_ = true;
            return TError("Error waiting for child process to finish: execve failed")
                << TError::FromSystem(errCode);
        }
    }

    ProcessId_ = pid;
    return TError();
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
        return TError::FromSystem();
    }

    YCHECK(result == ProcessId_);

    return StatusToError(Status_);
#endif
}

const char* TProcess::GetPath() const
{
    return Path_.data();
}

int TProcess::GetProcessId() const
{
    return ProcessId_;
}

char* TProcess::Capture(TStringBuf arg)
{
    std::vector<char> holder(arg.data(), arg.data() + arg.length());
    if (holder.empty() || (holder.back() != '\0')) {
        holder.push_back('\0');
    }
    StringHolder_.push_back(std::move(holder));
    return StringHolder_.back().data();
}

int TProcess::DoSpawn()
{
    YASSERT(ChildPipe_.WriteFd != InvalidFd);

    ::execve(Path_.data(), Args_.data(), Env_.data());

    const int errorCode = errno;
    while (::write(ChildPipe_.WriteFd, &errorCode, sizeof(int)) < 0);

    _exit(1);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT
