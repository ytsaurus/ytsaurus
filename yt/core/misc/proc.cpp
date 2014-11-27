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
        path = Format("/proc/%v/statm", pid);
    }

    TIFStream memoryStatFile(path);
    auto memoryStatFields = splitStroku(memoryStatFile.ReadLine(), " ");
    return FromString<i64>(memoryStatFields[1]) * NSystemInfo::GetPageSize();
#else
    return 0;
#endif
}

#ifdef _unix_

void RunCleaner(const Stroka& path)
{
    LOG_INFO("Clean %Qs", path);

    TProcess process(GetExecPath());
    process.AddArgument(AsStringBuf("--cleaner"));
    process.AddArgument(AsStringBuf("--dir-to-remove"));
    process.AddArgument(path);

    process.Spawn();
    auto error = process.Wait();
    THROW_ERROR_EXCEPTION_IF_FAILED(error);
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
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        int signalNumber = WTERMSIG(status);
        return TError(
            EExitStatus::SignalBase + signalNumber,
            "Process terminated by signal %v",
            signalNumber);
    } else if (WIFSTOPPED(status)) {
        int signalNumber = WSTOPSIG(status);
        return TError(
            EExitStatus::SignalBase + signalNumber,
            "Process stopped by signal %v",
            signalNumber);
    } else if (WIFEXITED(status)) {
        int exitCode = WEXITSTATUS(status);
        return TError(
            EExitStatus::ExitCodeBase + exitCode,
            "Process exited with code %v",
            exitCode);
    } else {
        return TError("Unknown status %v", status);
    }
}

void CloseAllDescriptors()
{
#ifdef _linux_
    DIR* dp = ::opendir("/proc/self/fd");
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
            case EINTR:
                continue;
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

#else

bool TryClose(int fd)
{
    UNUSED(fd);
    YUNIMPLEMENTED();
}

void SafeClose(int fd)
{
    UNUSED(fd);
    YUNIMPLEMENTED();
}

bool TryDup2(int oldFd, int newFd)
{
    UNUSED(oldFd);
    UNUSED(newFd);
    YUNIMPLEMENTED();
}

void SafeDup2(int oldFd, int newFd)
{
    UNUSED(oldFd);
    UNUSED(newFd);
    YUNIMPLEMENTED();
}

bool TryExecve(const char *path, const char* argv[], const char* env[])
{
    UNUSED(path);
    UNUSED(argv);
    UNUSED(env);
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

void RunCleaner(const Stroka& path)
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

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
