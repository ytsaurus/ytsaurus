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

i64 GetUserRss(int uid)
{
    YCHECK(uid > 0);

    LOG_DEBUG("Started computing RSS (UID: %d)", uid);

    auto pids = GetPidsByUid(uid);
    i64 result = 0;
    for (int pid : pids) {
        try {
            i64 rss = GetProcessRss(pid);
            LOG_DEBUG("PID: %d, RSS: %" PRId64,
                pid,
                rss);
            result += rss;
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Failed to get RSS for PID %d",
                pid);
        }
    }

    LOG_DEBUG("Finished computing RSS (UID: %d, RSS: %" PRId64 ")",
        uid,
        result);

    return result;
}

// The caller must be sure that it has root privileges.
void RunKiller(int uid)
{
    LOG_INFO("Kill %d processes", uid);
    YCHECK(uid > 0);

    auto throwError = [=] (const TError& error) {
        THROW_ERROR_EXCEPTION(
            "Failed to kill processes owned by %d.",
            uid) << error;
    };

    while (true) {
        TProcess process(~GetExecPath());
        process.AddArgument("--killer");
        process.AddArgument("--uid");
        process.AddArgument(~ToString(uid));

        auto pids = GetPidsByUid(uid);
        if (pids.empty())
            return;

        // We are forking here in order not to give the root privileges to the parent process ever,
        // because we cannot know what other threads are doing.
        auto error = process.Spawn();
        if (!error.IsOK()) {
            throwError(error);
        }

        error = process.Wait();
        if (!error.IsOK()) {
            throwError(error);
        }

        ThreadYield();
    }
}

void KillallByUid(int uid)
{
    auto pids = GetPidsByUid(uid);
    if (pids.empty())
        return;

    LOG_DEBUG("Killing processes (UID: %d, PIDs: [%s])",
              uid,
              ~JoinToString(pids));

    YCHECK(setuid(0) == 0);

    for (int pid : pids) {
        auto result = kill(pid, 9);
        if (result == -1) {
            YCHECK(errno == ESRCH);
        }
    }
}

void RunCleaner(const Stroka& path)
{
    LOG_INFO("Clean %s", ~path);

    TProcess process(~GetExecPath());
    process.AddArgument("--cleaner");
    process.AddArgument("--dir-to-remove");
    process.AddArgument(~path);

    auto throwError = [=] (const TError& error) {
        THROW_ERROR_EXCEPTION(
            "Failed to remove directory %s: %s",
            ~path) << error;
    };

    auto error = process.Spawn();
    if (!error.IsOK()) {
        throwError(error);
    }

    error = process.Wait();
    if (!error.IsOK()) {
        throwError(error);
    }
}

void RemoveDirAsRoot(const Stroka& path)
{
    // Child process
    YCHECK(setuid(0) == 0);
    execl("/bin/rm", "/bin/rm", "-rf", ~path, (void*)nullptr);

    THROW_ERROR_EXCEPTION("Failed to remove directory %s: %s",
        ~path,
        "execl failed") << TError::FromSystem();
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

#else

void KillallByUid(int uid)
{
    UNUSED(uid);
    YUNIMPLEMENTED();
}

void RunKiller(int uid)
{
    UNUSED(uid);
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
