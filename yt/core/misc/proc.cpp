#include "stdafx.h"
#include "proc.h"
#include "string.h"

#include <core/logging/log.h>
#include <core/misc/string.h>

#include <core/ytree/convert.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#include <util/system/yield.h>
#include <util/system/info.h>

#ifdef _unix_
    #include <spawn.h>
    #include <stdio.h>
    #include <dirent.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <sys/wait.h>
    #include <unistd.h>
#endif

namespace NYT {

static NLog::TLogger SILENT_UNUSED Logger("Proc");

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetPidsByUid(int uid)
{
#ifdef _linux_
    std::vector<int> result;

    DIR *dp = ::opendir("/proc");
    YCHECK(dp != nullptr);

    struct dirent *ep;
    while ((ep = ::readdir(dp)) != nullptr) {
        const char* begin = ep->d_name;
        char* end = nullptr;
        int pid = static_cast<int>(strtol(begin, &end, 10));
        if (begin == end) {
            // Not a pid.
            continue;
        }

        auto path = Sprintf("/proc/%d", pid);
        struct stat buf;
        int res = ::stat(~path, &buf);

        if (res == 0) {
            if (buf.st_uid == uid) {
                result.push_back(pid);
            }
        } else {
            // Assume that the process has already completed.
            auto errno_ = errno;
            LOG_DEBUG(TError::FromSystem(), "Failed to get UID for PID %d: stat failed",
                pid);
            YCHECK(errno_ == ENOENT || errno_ == ENOTDIR);
        }
    }

    YCHECK(::closedir(dp) == 0);
    return result;

#else
    return std::vector<int>();
#endif
}

i64 GetProcessRss(int pid)
{
#ifdef _linux_
    Stroka path = "/proc/self/statm";
    if (pid != -1) {
        path = Sprintf("/proc/%d/statm", pid);
    }

    TIFStream memoryStatFile(path);
    auto memoryStatFields = splitStroku(memoryStatFile.ReadLine(), " ");
    return FromString<i64>(memoryStatFields[1]) * NSystemInfo::GetPageSize();
#else
    return 0;
#endif
}

#ifdef _unix_

// The caller must be sure that it has root privileges.
void KillAll(TCallback<std::vector<int>()> pidsGetter)
{
    auto pidsToKill = pidsGetter.Run();
    if (pidsToKill.empty()) {
        return;
    }

    while (true) {
        auto pids = pidsGetter.Run();
        if (pids.empty())
            break;

        LOG_DEBUG("Killing processes (PIDs: [%s])",
            ~JoinToString(pids));

        // We are forking here in order not to give the root privileges to the parent process ever,
        // because we cannot know what other threads are doing.
        int forkedPid = fork();
        if (forkedPid < 0) {
            THROW_ERROR_EXCEPTION("Failed to kill processes: fork failed")
                << TError::FromSystem();
        }

        if (forkedPid == 0) {
            // In child process.
            YCHECK(setuid(0) == 0);

            FOREACH (int pid, pids) {
                auto result = kill(pid, 9);
                if (result == -1) {
                    YCHECK(errno == ESRCH);
                }
            }

            _exit(0);
        }

        // In parent process.

        int status = 0;
        {
            int result = waitpid(forkedPid, &status, WUNTRACED);
            if (result < 0) {
                THROW_ERROR_EXCEPTION("Failed to kill processes: waitpid failed")
                    << TError::FromSystem();
            }
            YCHECK(result == forkedPid);
        }

        auto statusError = StatusToError(status);
        if (!statusError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to kill processes: killer failed")
                << statusError;
        }

        ThreadYield();
    }
}

void RemoveDirAsRoot(const Stroka& path)
{
    // Allocation after fork can lead to a deadlock inside LFAlloc.
    // To avoid allocation we list contents of the directory before fork.

    auto pid = fork();
    // We are forking here in order not to give the root privileges to the parent process ever,
    // because we cannot know what other threads are doing.
    if (pid == 0) {
        // Child process
        YCHECK(setuid(0) == 0);
        execl("/bin/rm", "/bin/rm", "-rf", ~path, (void*)nullptr);

        fprintf(
            stderr,
            "Failed to remove directory (/bin/rm -rf %s): %s",
            ~path,
            ~ToString(TError::FromSystem()));
        _exit(1);
    }

    auto throwError = [=] (const Stroka& msg, const TError& error) {
        THROW_ERROR_EXCEPTION(
            "Failed to remove directory %s: %s",
            ~path,
            ~msg) << error;
    };

    // Parent process
    if (pid < 0) {
        throwError("fork failed", TError::FromSystem());
    }

    int status = 0;
    {
        int result = waitpid(pid, &status, WUNTRACED);
        if (result < 0) {
            throwError("waitpid failed", TError());
        }
        YCHECK(result == pid);
    }

    auto statusError = StatusToError(status);
    if (!statusError.IsOK()) {
        throwError("invalid exit status", statusError);
    }
}

TError StatusToError(int status)
{
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        int signalNumber = WTERMSIG(status);
        return TError(EExitStatus::SignalBase + signalNumber, "Process terminated by signal %d", signalNumber);
    } else if (WIFSTOPPED(status)) {
        int signalNumber = WSTOPSIG(status);
        return TError(EExitStatus::SignalBase + signalNumber, "Process stopped by signal %d", signalNumber);
    } else if (WIFEXITED(status)) {
        int exitCode = WEXITSTATUS(status);
        return TError(EExitStatus::ExitCodeBase + exitCode, "Process exited with code %d", exitCode);
    } else {
        return TError("Unknown status %d", status);
    }
}

void CloseAllDescriptors()
{
#ifdef _linux_
    DIR *dp = ::opendir("/proc/self/fd");
    YCHECK(dp != NULL);

    int dirfd = ::dirfd(dp);
    YCHECK(dirfd >= 0);

    struct dirent *ep;
    while ((ep = ::readdir(dp)) != nullptr) {
        char* begin = ep->d_name;
        char* end = nullptr;
        int fd = static_cast<int>(strtol(begin, &end, 10));
        if (fd != dirfd && begin != end) {
            YCHECK(::close(fd) == 0);
        }
    }

    YCHECK(::closedir(dp) == 0);
#endif
}

void SafeClose(int fd, bool ignoreInvalidFd)
{
    while (true) {
        auto res = close(fd);
        if (res == -1) {
            switch (errno) {
            case EINTR:
                break;

            case EBADF:
                if (ignoreInvalidFd) {
                    return;
                } // otherwise fall through and throw exception.

            default:
                THROW_ERROR_EXCEPTION("close failed")
                    << TError::FromSystem();
            }
        } else {
            return;
        }
    }
}

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

int getErrNoFromExitCode(int exitCode) {
    int index = BASE_EXIT_CODE - exitCode;
    if (index >= 0) {
        return EXEC_ERR_CODE[index];
    }
    return 0;
}

int Spawn(const char* path, std::vector<Stroka>& arguments)
{
    std::vector<char *> args;
    FOREACH (auto& x, arguments) {
        args.push_back(x.begin());
    }
    args.push_back(NULL);

    int pid = vfork();
    if (pid < 0) {
        THROW_ERROR_EXCEPTION("Error starting child process: vfork failed")
            << TErrorAttribute("path", path)
            << TErrorAttribute("arguments", arguments)
            << TError::FromSystem(pid);
    }

    if (pid == 0) {
        execvp(path, &args[0]);
        const int errorCode = errno;
        int i = 0;
        while ((EXEC_ERR_CODE[i] != errorCode) && (EXEC_ERR_CODE[i] != 0)) {
            ++i;
        }

        _exit(BASE_EXIT_CODE - i);
    }

    return pid;
}

#else

void KillAll(TCallback<std::vector<int>()>)
{
    YUNIMPLEMENTED();
}

TError StatusToError(int status)
{
    UNUSED(status);
    YUNIMPLEMENTED();
}

void RemoveDirAsRoot(const Stroka& path)
{
    UNUSED(path);
    YUNIMPLEMENTED();
}

void CloseAllDescriptors()
{
    YUNIMPLEMENTED();
}

void SafeClose(int fd, bool ignoreInvalidFd)
{
    YUNIMPLEMENTED();
}

int Spawn(const char* path, std::vector<Stroka>& arguments)
{
    UNUSED(path);
    UNUSED(arguments);
    YUNIMPLEMENTED();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
