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

////////////////////////////////////////////////////////////////////////////////

TProcess::TProcess(const char* path)
    : IsFinished_(false)
    , ProcessId_(-1)
    , Stack_(4096, 0)
{
    AddArgument(path);
}

void TProcess::AddArgument(const char* arg)
{
    size_t size = strlen(arg);
    Holder_.push_back(std::vector<char>(arg, arg + size));
    Args_.push_back(&(Holder_[Holder_.size() - 1].front()));
}

TError TProcess::Spawn()
{
    YCHECK((ProcessId_ != -1) && !IsFinished_);
    Args_.push_back(nullptr);

    int pid = vfork();
    if (pid < 0) {
        return TError("Error starting child process: vfork failed")
            << TErrorAttribute("path", GetPath())
            << TError::FromSystem();
    }

    if (pid == 0) {
        execvp(Args_.front(), &(Args_.front()));
        const int errorCode = errno;
        int i = 0;
        while ((EXEC_ERR_CODE[i] != errorCode) && (EXEC_ERR_CODE[i] != 0)) {
            ++i;
        }

        _exit(BASE_EXIT_CODE - i);
    }
    ProcessId_ = pid;
    return TError();
}

TError TProcess::Wait()
{
    YCHECK(ProcessId_ != -1);

    int status = 0;
    int result = waitpid(ProcessId_, &status, WUNTRACED);
    IsFinished_ = true;

    if (result < 0) {
        return TError::FromSystem();
    }
    YCHECK(result == ProcessId_);
    return StatusToError(status);
}

const char* TProcess::GetPath() const
{
    return Args_.front();
}

int TProcess::GetProcessId() const
{
    return ProcessId_;
}

} // NYT
