#include "process.h"
#include "proc.h"

#include <core/misc/error.h>

#include <string.h>
#include <unistd.h>
#include <errno.h>

#include <sys/wait.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const int BASE_EXIT_CODE = 127;
static const int EXEC_ERR_CODE[] = {
    E2BIG,
    EACCES,
    EFAULT,
    EINVAL,
    EIO,
    EISDIR,
#ifdef _linux_
    ELIBBAD,
#endif
    ELOOP,
    EMFILE,
    ENAMETOOLONG,
    ENFILE,
    ENOENT,
    ENOEXEC,
    ENOMEM,
    ENOTDIR,
    EPERM,
    ETXTBSY,
    0
};
static const size_t StackSize = 4096;

////////////////////////////////////////////////////////////////////////////////

int child(void* this_)
{
    TProcess* process = static_cast<TProcess*>(this_);
    return process->DoSpawn();
}

const char* GetFilename(const char* path)
{
    const char* name = strrchr(path, '/');
    if (name == nullptr) {
        name = path;
    } else {
        // point after '/'
        ++name;
    }
    return name;
}

////////////////////////////////////////////////////////////////////////////////

TProcess::TProcess(const char* path)
    : IsFinished_(false)
    , Status_(0)
    , ProcessId_(-1)
    , Pipe_{-1, -1}
    , Stack_(StackSize, 0)
{
    size_t size = strlen(path);
    Path_.insert(Path_.end(), path, path + size + 1);

    AddArgument(GetFilename(path));
}

void TProcess::AddArgument(const char* arg)
{
    YCHECK((ProcessId_ == -1) && !IsFinished_);

    Args_.push_back(Copy(arg));
}

TError TProcess::Spawn()
{
    YCHECK((ProcessId_ == -1) && !IsFinished_);

    auto res = pipe(Pipe_);
    if (res == -1) {
        ClosePipe();
        return TError("Unable to creation failed") << TError::FromSystem();
    }

    for (int i = 0; i < 2; ++i) {
        res = ::fcntl(Pipe_[i], F_GETFL);

        if (res == -1) {
            ClosePipe();
            return TError("fcntl failed to get descriptor flags")
                << TError::FromSystem();
        }

        res = ::fcntl(Pipe_[i], F_SETFL, res | O_CLOEXEC);

        if (res == -1) {
            return TError("fcntl failed to set descriptor flags")
                << TError::FromSystem();
        }
    }

    // copy env
    char** iterator = environ;
    while (*iterator != 0) {
        const char* const item = (*iterator);
        Env_.push_back(Copy(item));

        ++iterator;
    }
    Env_.push_back(nullptr);
    Args_.push_back(nullptr);

    int pid = ::clone(child,
        (&Stack_.front()) + Stack_.size(),
        CLONE_VM|SIGCHLD,
        this);

    ::close(Pipe_[1]);

    if (pid < 0) {
        ::close(Pipe_[0]);
        Pipe_[0] = -1;

        return TError("Error starting child process: clone failed")
            << TErrorAttribute("path", GetPath())
            << TError::FromSystem();
    }

    ProcessId_ = pid;
    return TError();
}

int TProcess::DoSpawn()
{
    ::close(Pipe_[0]);
    ::execve(&Path_.front(), &(Args_.front()), &(Env_.front()));
    const int errorCode = errno;
    int i = 0;
    while ((EXEC_ERR_CODE[i] != errorCode) && (EXEC_ERR_CODE[i] != 0)) {
        ++i;
    }

    while (::write(Pipe_[1], &errorCode, sizeof(int)) < 0);

    _exit(BASE_EXIT_CODE - i);
}

TError TProcess::Wait()
{
    YCHECK(ProcessId_ != -1);
    YCHECK(Pipe_[0] != -1);

    {
        int errCode;
        if (::read(Pipe_[0], &errCode, sizeof(int)) != sizeof(int)) {
            errCode = 0;
        } else {
            ::waitpid(ProcessId_, nullptr, 0);
            ::close(Pipe_[0]);
            Pipe_[0] = Pipe_[1] = -1;
            IsFinished_ = true;
            return TError("execve failed") << TError::FromSystem(errCode);
        }
    }

    int result = ::waitpid(ProcessId_, &Status_, WUNTRACED);
    IsFinished_ = true;

    if (result < 0) {
        return TError::FromSystem();
    }
    YCHECK(result == ProcessId_);
    return StatusToError(Status_);
}

const char* TProcess::GetPath() const
{
    return &Path_.front();
}

int TProcess::GetProcessId() const
{
    return ProcessId_;
}

char* TProcess::Copy(const char* arg)
{
    size_t size = strlen(arg);
    Holder_.push_back(std::vector<char>(arg, arg + size + 1));
    return &(Holder_[Holder_.size() - 1].front());
}

void TProcess::ClosePipe()
{
    if (Pipe_[0] != -1) {
        ::close(Pipe_[0]);
    }
    if (Pipe_[1] != -1) {
        ::close(Pipe_[1]);
    }
    Pipe_[0] = Pipe_[1] = -1;
}

} // NYT
