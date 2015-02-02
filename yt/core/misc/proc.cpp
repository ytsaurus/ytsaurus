#include "stdafx.h"
#include "proc.h"
#include "string.h"
#include "process.h"

#include <core/logging/log.h>

#include <core/misc/string.h>

#include <core/ytree/convert.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#include <util/system/yield.h>
#include <util/system/info.h>
#include <util/system/execpath.h>

#ifdef _unix_
    #include <stdio.h>
    #include <dirent.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <unistd.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const NLog::TLogger Logger("Proc");

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetPidsByUid(int uid)
{
#ifdef _linux_
    std::vector<int> result;

    DIR *dirStream = ::opendir("/proc");
    YCHECK(dirStream != nullptr);

    struct dirent *ep;
    while ((ep = ::readdir(dirStream)) != nullptr) {
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

    YCHECK(::closedir(dirStream) == 0);
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
        path = Format("/proc/%v/statm", pid);
    }

    TIFStream memoryStatFile(path);
    auto memoryStatFields = splitStroku(memoryStatFile.ReadLine(), " ");
    return FromString<i64>(memoryStatFields[1]) * NSystemInfo::GetPageSize();
#else
    return 0;
#endif
}

void PrepareUserJobPipe(int fd, int permissions)
{
#ifdef _unix_
    auto procPath = Format("/proc/self/fd/%v", fd);
    auto res = chmod(~procPath, permissions);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("Failed to chmod job descriptor")
            << TErrorAttribute("fd", fd)
            << TErrorAttribute("permissions", permissions)
            << TError::FromSystem();
    }
#endif
}

#ifdef _unix_

void RunCleaner(const Stroka& path)
{
    LOG_INFO("Clean %Qs", path);

    TProcess process(GetExecPath());
    process.AddArguments({
        "--cleaner",
        "--dir-to-remove",
        path
    });

    process.Spawn();
    process.Wait().ThrowOnError();
}

void RemoveDirAsRoot(const Stroka& path)
{
    // Child process
    YCHECK(setuid(0) == 0);
    execl("/bin/rm", "/bin/rm", "-rf", path.c_str(), (void*)nullptr);

    THROW_ERROR_EXCEPTION("Failed to remove directory %Qv: execl failed",
        path) << TError::FromSystem();
}

TError StatusToError(int status)
{
    int signalBase = static_cast<int>(EExitStatus::SignalBase);
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        int signalNumber = WTERMSIG(status);
        return TError(
            signalBase + signalNumber,
            "Process terminated by signal %v",
            signalNumber);
    } else if (WIFSTOPPED(status)) {
        int signalNumber = WSTOPSIG(status);
        return TError(
            signalBase + signalNumber,
            "Process stopped by signal %v",
            signalNumber);
    } else if (WIFEXITED(status)) {
        int exitCode = WEXITSTATUS(status);
        return TError(
            signalBase + exitCode,
            "Process exited with code %v",
            exitCode);
    } else {
        return TError("Unknown status %v", status);
    }
}

bool TryExecve(const char *path, char* const argv[], char* const env[])
{
    ::execve(path, argv, env);
    // If we are still here, it's an error.
    return false;
}

bool TryDup2(int oldFd, int newFd)
{
    while (true) {
        auto res = ::dup2(oldFd, newFd);

        if (res != -1) {
            return true;
        }

        if (errno == EINTR || errno == EBUSY) {
            continue;
        }

        return false;
    }
}

bool TryClose(int fd)
{
    while (true) {
        auto res = ::close(fd);
        if (res != -1) {
            return true;
        }

        switch (errno) {
            // Please read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing.
            case EINTR:
            // If the descriptor is no longer valid, just ignore it.
            case EBADF:
                return true;
            default:
                return false;
        }
    }
}

void SafeClose(int fd)
{
    if (!TryClose(fd)) {
        THROW_ERROR TError::FromSystem();
    }
}

void SafeDup2(int oldFd, int newFd)
{
    if (!TryDup2(oldFd, newFd)) {
        THROW_ERROR_EXCEPTION("dup2 failed")
            << TErrorAttribute("old_fd", oldFd)
            << TErrorAttribute("new_fd", newFd)
            << TError::FromSystem();
    }
}

void SetPermissions(int fd, int permissions)
{
    auto procPath = Format("/proc/self/fd/%v", fd);
    auto res = chmod(~procPath, permissions);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("Failed to set permissions for descriptor")
            << TErrorAttribute("fd", fd)
            << TErrorAttribute("permissions", permissions)
            << TError::FromSystem();
    }
}

void SafePipe(int fd[2])
{
    auto res = pipe(fd);
    if (res == -1) {
        THROW_ERROR_EXCEPTION("pipe failed")
            << TError::FromSystem();
    }
}

void SafeMakeNonblocking(int fd)
{
    auto res = fcntl(fd, F_GETFL);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("fcntl failed to get descriptor flags")
            << TError::FromSystem();
    }

    res = fcntl(fd, F_SETFL, res | O_NONBLOCK);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("fcntl failed to set descriptor flags")
            << TError::FromSystem();
    }
}

#else

bool TryClose(int /* fd */)
{
    YUNIMPLEMENTED();
}

void SafeClose(int /* fd */)
{
    YUNIMPLEMENTED();
}

bool TryDup2(int /* oldFd */, int /* newFd */)
{
    YUNIMPLEMENTED();
}

void SafeDup2(int /* oldFd */, int /* newFd */)
{
    YUNIMPLEMENTED();
}

bool TryExecve(const char /* *path */, const char* /* argv[] */, const char* /* env[] */)
{
    YUNIMPLEMENTED();
}

TError StatusToError(int /* status */)
{
    YUNIMPLEMENTED();
}

void RemoveDirAsRoot(const Stroka& /* path */)
{
    YUNIMPLEMENTED();
}

void RunCleaner(const Stroka& /* path */)
{
    YUNIMPLEMENTED();
}

void CloseAllDescriptors()
{
    YUNIMPLEMENTED();
}

void SetPermissions(int /* fd */, int /* permissions */)
{
    YUNIMPLEMENTED();
}

void SafePipe(int /* fd */ [2])
{
    YUNIMPLEMENTED();
}

void SafeMakeNonblocking(int /* fd */)
{
    YUNIMPLEMENTED();
}

#endif

void CloseAllDescriptors(const std::vector<int>& exceptFor)
{
#ifdef _linux_
    auto* dirStream = ::opendir("/proc/self/fd");
    YCHECK(dirStream != NULL);

    int dirFd = ::dirfd(dirStream);
    YCHECK(dirFd >= 0);

    dirent* ep;
    while ((ep = ::readdir(dirStream)) != nullptr) {
        char* begin = ep->d_name;
        char* end = nullptr;
        int fd = static_cast<int>(strtol(begin, &end, 10));
        if (begin == end)
            continue;
        if (fd == dirFd)
            continue;
        if (std::find(exceptFor.begin(), exceptFor.end(), fd) != exceptFor.end())
            continue;
        YCHECK(::close(fd) == 0);
    }

    YCHECK(::closedir(dirStream) == 0);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
