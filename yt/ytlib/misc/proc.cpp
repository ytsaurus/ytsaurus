#include "stdafx.h"

#include "proc.h"

#include <util/stream/file.h>
#include <util/string/vector.h>

#ifdef _unix_

#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>

#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

i64 GetProcessRss(int pid)
{
    Stroka path = "/proc/self/statm";
    if (pid != -1) {
        path = Sprintf("/proc/%d/statm", pid);
    }

    TIFStream memoryStatFile(path);
    auto memoryStatFields = splitStroku(memoryStatFile.ReadLine(), " ");
    return FromString<i64>(memoryStatFields[1]);
}

#ifdef _unix_

i64 GetUserRss(int uid)
{
    YCHECK(uid > 0);
    // Column of rss of all processes for given user in kb.
    auto command = Sprintf("ps -u %d -o rss --no-headers", uid);

    // Read + close on exec.
    FILE *fd = popen(~command, "re");

    if (!fd) {
        THROW_ERROR_EXCEPTION(
            "Failed to get memory usage for UID %d: popen failed",
            uid) << TError::FromSystem();
    }

    i64 result = 0;
    int rss;
    int n;
    while ((n = fscanf(fd, "%d", &rss)) != EOF) {
        if (n = 1) {
            result += rss;
        } else {
            THROW_ERROR_EXCEPTION(
                "Failed to get memory usage for UID %d: fscanf failed",
                uid) << TError::FromSystem();
        }
    }

    // ToDo(psushin): consider checking pclose errors.
    pclose(fd);
    return result * 1024;
}

int GuardedFork()
{
    //ToDo(psushin): Remove this mutex when libc is fixed.
    static TMutex ForkMutex;
    ForkMutex.Acquire();
    auto pid = fork();
    if (pid != 0) {
        ForkMutex.Release();
    }
    return pid;
}

// The caller must be sure that it has root privileges.
void KillallByUser(int uid)
{
    YCHECK(uid > 0);
    auto pid = GuardedFork();
    if (pid == 0) {
        // Child process
        YCHECK(setuid(0) == 0);
        YCHECK(setuid(uid) == 0);
        // Send sigkill to all available processes.
        auto res = kill(-1, 9);
        if (res == -1) {
            YCHECK(errno == ESRCH);
        }
        _exit(0);
    }

    // Parent process
    if (pid < 0) {
        THROW_ERROR_EXCEPTION(
            "Failed to kill processes for uid %d: fork failed",
            uid) << TError::FromSystem();
    }

    int status = 0;
    {
        int result = waitpid(pid, &status, WUNTRACED);
        if (result < 0) {
            THROW_ERROR_EXCEPTION(
                "Failed to kill processes for uid %d: waitpid failed",
                uid) << TError::FromSystem();
        }
        YCHECK(result == pid);
    }

    auto statusError = StatusToError(status);
    if (!statusError.IsOK()) {
        THROW_ERROR_EXCEPTION(
            "Failed to kill processes for uid %d: waitpid failed",
            uid) << statusError;
    }
}

TError StatusToError(int status)
{
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        return TError("Process terminated by signal %d",  WTERMSIG(status));
    } else if (WIFSTOPPED(status)) {
        return TError("Process stopped by signal %d",  WSTOPSIG(status));
    } else if (WIFEXITED(status)) {
        return TError("Process exited with value %d",  WEXITSTATUS(status));
    } else {
        return TError("Unknown status %d", status);
    }
}

#else

int GuardedFork()
{
    YUNIMPLEMENTED();
}

void KillallByUser(int uid)
{
    YUNIMPLEMENTED();
}

TError StatusToError(int status)
{
    YUNIMPLEMENTED();
}

i64 GetUserRss(int uid)
{
    YUNIMPLEMENTED();
}

#endif


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
