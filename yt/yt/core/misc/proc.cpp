#include "proc.h"
//#include "process.h"
#include "string.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/fs.h>

#include <util/stream/file.h>

#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/vector.h>

#include <util/system/info.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/folder/iterator.h>

#ifdef _unix_
    #include <stdio.h>
    #include <dirent.h>
    #include <errno.h>
    #include <pwd.h>
    #include <sys/ioctl.h>
    #include <sys/types.h>
    #include <sys/resource.h>
    #include <sys/stat.h>
    #include <sys/syscall.h>
    #include <sys/ttydefaults.h>
    #include <unistd.h>
#endif
#ifdef _linux_
    #include <pty.h>
    #include <pwd.h>
    #include <grp.h>
    #include <utmp.h>
    #include <sys/prctl.h>
    #include <sys/sysmacros.h>
    #include <sys/ttydefaults.h>
#endif
#ifdef _darwin_
    #include <util.h>
    #include <pthread.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Proc");

////////////////////////////////////////////////////////////////////////////////

bool IsSystemErrorCode(TErrorCode errorCode)
{
    return errorCode >= LinuxErrorCodeBase && errorCode < LinuxErrorCodeBase + LinuxErrorCodeCount;
}

bool IsSystemError(const TError& error)
{
    if (IsSystemErrorCode(error.GetCode())) {
        return true;
    }
    for (const auto& innerError : error.InnerErrors()) {
        if (IsSystemError(innerError)) {
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<int> GetParentPid(int pid)
{
    TFileInput in(Format("/proc/%v/status", pid));
    TString line;
    while (in.ReadLine(line)) {
        const TString ppidMarker = "PPid:\t";
        if (line.StartsWith(ppidMarker)) {
            line = line.substr(ppidMarker.size());
            return FromString<int>(line);
        }
    }

    return {};
}

std::vector<int> ListPids()
{
#ifdef _linux_
    std::vector<int> pids;

    DIR* dirStream = ::opendir("/proc");
    YT_VERIFY(dirStream != nullptr);

    struct dirent* ep;
    while ((ep = ::readdir(dirStream)) != nullptr) {
        const char* begin = ep->d_name;
        char* end = nullptr;
        int pid = static_cast<int>(strtol(begin, &end, 10));
        if (begin == end) {
            // Not a pid.
            continue;
        }
        pids.push_back(pid);
    }

    YT_VERIFY(::closedir(dirStream) == 0);
    return pids;
#else
    return {};
#endif
}

std::vector<int> GetPidsUnderParent(int targetPid)
{
#ifdef _linux_
    std::vector<int> result;
    std::map<int, int> parents;

    DIR* dirStream = ::opendir("/proc");
    YT_VERIFY(dirStream != nullptr);

    struct dirent* ep;
    while ((ep = ::readdir(dirStream)) != nullptr) {
        const char* begin = ep->d_name;
        char* end = nullptr;
        int pid = static_cast<int>(strtol(begin, &end, 10));
        if (begin == end) {
            // Not a pid.
            continue;
        }

        auto path = Format("/proc/%v/status", pid);
        try {
            auto ppid = GetParentPid(pid);
            if (ppid) {
                parents[pid] = *ppid;
            }
        } catch(...) {
            // Assume that the process has already completed.
            continue;
        }
    }

    for (auto [pid, ppid] : parents) {
        while (true) {
            if (ppid == targetPid) {
                result.push_back(pid);
            }

            auto it = parents.find(ppid);
            if (it == parents.end()) {
                break;
            } else {
                ppid = it->second;
            }
        }
    }

    YT_VERIFY(::closedir(dirStream) == 0);
    return result;

#else
    return {};
#endif
}

std::vector<int> GetPidsByUid(int uid)
{
#ifdef _linux_
    std::vector<int> result;

    DIR* dirStream = ::opendir("/proc");
    YT_VERIFY(dirStream != nullptr);

    struct dirent* ep;
    while ((ep = ::readdir(dirStream)) != nullptr) {
        const char* begin = ep->d_name;
        char* end = nullptr;
        int pid = static_cast<int>(strtol(begin, &end, 10));
        if (begin == end) {
            // Not a pid.
            continue;
        }

        auto path = Format("/proc/%v", pid);
        struct stat buf;
        int res = ::stat(path.data(), &buf);

        if (res == 0) {
            if (buf.st_uid == uid || uid == -1) {
                result.push_back(pid);
            }
        } else {
            // Assume that the process has already completed.
            auto errno_ = errno;
            YT_LOG_DEBUG(TError::FromSystem(), "Failed to get UID for PID %v: stat failed",
                pid);
            YT_VERIFY(errno_ == ENOENT || errno_ == ENOTDIR);
        }
    }

    YT_VERIFY(::closedir(dirStream) == 0);
    return result;
#else
    return std::vector<int>();
#endif
}

size_t GetCurrentProcessId()
{
#if defined(_linux_)
    return getpid();
#else
    YT_ABORT();
#endif
}

size_t GetCurrentThreadId()
{
#if defined(_linux_)
    return static_cast<size_t>(::syscall(SYS_gettid));
#elif defined(_darwin_)
    uint64_t tid;
    YT_VERIFY(pthread_threadid_np(nullptr, &tid) == 0);
    return static_cast<size_t>(tid);
#else
    YT_ABORT();
#endif
}

void ChownChmodDirectory(const TString& path, const std::optional<uid_t>& userId, const std::optional<int>& permissions)
{
#ifdef _unix_
    if (userId) {
        auto res = HandleEintr(::chown, path.data(), *userId, -1);
        if (res != 0) {
            THROW_ERROR_EXCEPTION("Failed to change owner for directory %v", path)
                << TErrorAttribute("owner_uid", *userId)
                << TError::FromSystem();
        }
    }

    if (permissions) {
        auto res = HandleEintr(::chmod, path.data(), *permissions);
        if (res != 0) {
            THROW_ERROR_EXCEPTION("Failed to set permissions for directory %v", path)
                << TErrorAttribute("permissions", *permissions)
                << TError::FromSystem();
        }
    }
#else
    YT_ABORT();
#endif
}

void ChownChmodDirectoriesRecursively(const TString& path, const std::optional<uid_t>& userId, const std::optional<int>& permissions)
{
#ifdef _unix_
    for (const auto& directoryPath : NFS::EnumerateDirectories(path)) {
        auto nestedPath = NFS::CombinePaths(path, directoryPath);
        ChownChmodDirectoriesRecursively(nestedPath, userId, permissions);
    }

    ChownChmodDirectory(path, userId, permissions);
#else
    YT_ABORT();
#endif
}

void SetThreadPriority(int tid, int priority)
{
#ifdef _unix_
    auto res = ::setpriority(PRIO_PROCESS, tid, priority);
    if (res != 0) {
        THROW_ERROR_EXCEPTION("Failed to set priority for thread %v",
            tid) << TError::FromSystem();
    }
#else
    YT_ABORT();
#endif
}

TMemoryUsage GetProcessMemoryUsage(int pid)
{
#ifdef _linux_
    TString path = "/proc/self/statm";
    if (pid != -1) {
        path = Format("/proc/%v/statm", pid);
    }

    TIFStream memoryStatFile(path);
    auto memoryStatFields = SplitString(memoryStatFile.ReadLine(), " ");
    return TMemoryUsage {
        FromString<ui64>(memoryStatFields[1]) * NSystemInfo::GetPageSize(),
        FromString<ui64>(memoryStatFields[2]) * NSystemInfo::GetPageSize(),
    };
#else
    return TMemoryUsage{0, 0};
#endif
}

THashMap<TString, i64> GetVmstat()
{
#ifdef _linux_
    THashMap<TString, i64> result;
    TString path = "/proc/vmstat";
    TFileInput vmstatFile(path);
    auto data = vmstatFile.ReadAll();
    auto lines = SplitString(data, "\n");
    for (const auto& line : lines) {
        auto strippedLine = Strip(line);
        if (strippedLine.empty()) {
            continue;
        }
        auto fields = SplitString(line, " ");
        result[fields[0]] = NSystemInfo::GetPageSize() * FromString<i64>(fields[1]);
    }
    return result;
#else
    return {};
#endif
}

ui64 GetProcessCumulativeMajorPageFaults(int pid)
{
#ifdef _linux_
    TString path = "/proc/self/stat";
    if (pid != -1) {
        path = Format("/proc/%v/stat", pid);
    }

    TIFStream statFile(path);
    auto statFields = SplitString(statFile.ReadLine(), " ");
    return FromString<ui64>(statFields[11]) + FromString<ui64>(statFields[12]);
#else
    return 0;
#endif
}

TString GetProcessName(int pid)
{
#ifdef _linux_
    TString path = Format("/proc/%v/comm", pid);
    return Trim(TUnbufferedFileInput(path).ReadAll(), "\n");
#else
    return "";
#endif
}

std::vector<TString> GetProcessCommandLine(int pid)
{
#ifdef _linux_
    TString path = Format("/proc/%v/cmdline", pid);
    auto raw = TUnbufferedFileInput(path).ReadAll();
    std::vector<TString> result;
    auto begin = 0;
    while (begin < raw.length()) {
        auto end = raw.find('\0', begin);
        if (end == TString::npos) {
            result.push_back(raw.substr(begin));
            begin = raw.length();
        } else {
            result.push_back(raw.substr(begin, end - begin));
            begin = end + 1;
        }
    }

    return result;
#else
    return std::vector<TString>();
#endif
}

#ifdef _unix_

TError StatusToError(int status)
{
    if (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) {
        return TError();
    } else if (WIFSIGNALED(status)) {
        int signalNumber = WTERMSIG(status);
        return TError(
            EProcessErrorCode::Signal,
            "Process terminated by signal %v",
            signalNumber)
            << TErrorAttribute("signal", signalNumber);
    } else if (WIFSTOPPED(status)) {
        int signalNumber = WSTOPSIG(status);
        return TError(
            EProcessErrorCode::Signal,
            "Process stopped by signal %v",
            signalNumber)
            << TErrorAttribute("signal", signalNumber);
    } else if (WIFEXITED(status)) {
        int exitCode = WEXITSTATUS(status);
        return TError(
            EProcessErrorCode::NonZeroExitCode,
            "Process exited with code %v",
            exitCode)
            << TErrorAttribute("exit_code", exitCode);
    } else {
        return TError("Unknown status %v", status);
    }
}

TError ProcessInfoToError(const siginfo_t& processInfo)
{
    switch (processInfo.si_code) {
        case CLD_EXITED: {
            auto exitCode = processInfo.si_status;
            if (exitCode == 0) {
                return TError();
            } else {
                return TError(
                    EProcessErrorCode::NonZeroExitCode,
                    "Process exited with code %v",
                    exitCode)
                    << TErrorAttribute("exit_code", exitCode);
            }
        }

        case CLD_KILLED:
        case CLD_DUMPED: {
            int signal = processInfo.si_status;
            return TError(
                EProcessErrorCode::Signal,
                "Process terminated by signal %v",
                signal)
                << TErrorAttribute("signal", signal)
                << TErrorAttribute("core_dumped", processInfo.si_code == CLD_DUMPED);
        }

        default:
            return TError("Unknown signal code %v",
                processInfo.si_code);
    }
}

bool TryExecve(const char* path, const char* const* argv, const char* const* env)
{
    ::execve(
        path,
        const_cast<char* const*>(argv),
        const_cast<char* const*>(env));
    // If we are still here, it's an error.
    return false;
}

bool TryDup2(int oldFD, int newFD)
{
    while (true) {
        auto res = HandleEintr(::dup2, oldFD, newFD);

        if (res != -1) {
            return true;
        }

        if (errno == EBUSY) {
            continue;
        }

        return false;
    }
}

bool TryClose(int fd, bool ignoreBadFD)
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
                return true;
            case EBADF:
                return ignoreBadFD;
            default:
                return false;
        }
    }
}

void SafeClose(int fd, bool ignoreBadFD)
{
    if (!TryClose(fd, ignoreBadFD)) {
        THROW_ERROR TError::FromSystem();
    }
}

void SafeDup2(int oldFD, int newFD)
{
    if (!TryDup2(oldFD, newFD)) {
        THROW_ERROR_EXCEPTION("dup2 failed")
            << TErrorAttribute("old_fd", oldFD)
            << TErrorAttribute("new_fd", newFD)
            << TError::FromSystem();
    }
}

void SafeSetCloexec(int fd)
{
    int getResult = ::fcntl(fd, F_GETFD);
    if (getResult == -1) {
        THROW_ERROR_EXCEPTION("Error creating pipe: fcntl failed to get descriptor flags")
            << TError::FromSystem();
    }

    int setResult = ::fcntl(fd, F_SETFD, getResult | FD_CLOEXEC);
    if (setResult == -1) {
        THROW_ERROR_EXCEPTION("Error creating pipe: fcntl failed to set descriptor flags")
            << TError::FromSystem();
    }
}

void SetUid(int uid)
{
    // Set unprivileged uid for user process.
    if (setuid(0) != 0) {
        THROW_ERROR_EXCEPTION("Unable to set zero uid")
            << TError::FromSystem();
    }

    errno = 0;
#ifdef _linux_
    const auto* passwd = getpwuid(uid);
    int gid = (passwd && errno == 0)
      ? passwd->pw_gid
      : uid; // fallback value.

    if (setresgid(gid, gid, gid) != 0) {
        THROW_ERROR_EXCEPTION("Unable to set gids")
                << TErrorAttribute("uid", uid)
                << TErrorAttribute("gid", gid)
                << TError::FromSystem();
    }

    if (setresuid(uid, uid, uid) != 0) {
        THROW_ERROR_EXCEPTION("Unable to set uids")
            << TErrorAttribute("uid", uid)
            << TError::FromSystem();
    }
#else
    if (setuid(uid) != 0) {
        THROW_ERROR_EXCEPTION("Unable to set uid")
            << TErrorAttribute("uid", uid)
            << TError::FromSystem();
    }

    if (setgid(uid) != 0) {
        THROW_ERROR_EXCEPTION("Unable to set gid")
            << TErrorAttribute("gid", uid)
            << TError::FromSystem();
    }
#endif
}

void SafePipe(int fd[2])
{
#ifdef _linux_
    auto result = ::pipe2(fd, O_CLOEXEC);
    if (result == -1) {
        THROW_ERROR_EXCEPTION("Error creating pipe")
            << TError::FromSystem();
    }
#else
    {
        int result = ::pipe(fd);
        if (result == -1) {
            THROW_ERROR_EXCEPTION("Error creating pipe")
                << TError::FromSystem();
        }
    }
    SafeSetCloexec(fd[0]);
    SafeSetCloexec(fd[1]);
#endif
}

int SafeDup(int fd)
{
    auto result = ::dup(fd);
    if (result == -1) {
        THROW_ERROR_EXCEPTION("Error duplicating fd")
            << TError::FromSystem();
    }
    return result;
}

void SafeOpenPty(int* masterFD, int* slaveFD, int height, int width)
{
#ifdef _linux_
    {
        struct termios tt = {};
        tt.c_iflag = TTYDEF_IFLAG & ~ISTRIP;
        tt.c_oflag = TTYDEF_OFLAG;
        tt.c_lflag = TTYDEF_LFLAG;
        tt.c_cflag = (TTYDEF_CFLAG & ~(CS7 | PARENB | HUPCL)) | CS8;
        tt.c_cc[VERASE] = '\x7F';
        cfsetispeed(&tt, B38400);
        cfsetospeed(&tt, B38400);

        struct winsize ws = {};
        struct winsize* wsPtr = nullptr;
        if (height > 0 && width > 0) {
            ws.ws_row = height;
            ws.ws_col = width;
            wsPtr = &ws;
        }

        int result = ::openpty(masterFD, slaveFD, nullptr, &tt, wsPtr);
        if (result == -1) {
            THROW_ERROR_EXCEPTION("Error creating pty: pty creation failed")
                << TError::FromSystem();
        }
    }
    SafeSetCloexec(*masterFD);
#else
    THROW_ERROR_EXCEPTION("Unsupported");
#endif
}

void SafeLoginTty(int slaveFD)
{
#ifdef _linux_
    int result = ::login_tty(slaveFD);
    if (result == -1) {
        THROW_ERROR_EXCEPTION("Error attaching pty to standard streams")
            << TError::FromSystem();
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported");
#endif
}

void SafeSetTtyWindowSize(int fd, int height, int width)
{
    if (height > 0 && width > 0) {
        struct winsize ws;
        int result = ::ioctl(fd, TIOCGWINSZ, &ws);
        if (result == -1) {
            THROW_ERROR_EXCEPTION("Error reading tty window size")
                << TError::FromSystem();
        }
        if (ws.ws_row != height || ws.ws_col != width) {
            ws.ws_row = height;
            ws.ws_col = width;
            result = ::ioctl(fd, TIOCSWINSZ, &ws);
            if (result == -1) {
                THROW_ERROR_EXCEPTION("Error setting tty window size")
                    << TError::FromSystem();
            }
        }
    }
}

bool TryMakeNonblocking(int fd)
{
    auto res = fcntl(fd, F_GETFL);

    if (res == -1) {
        return false;
    }

    res = fcntl(fd, F_SETFL, res | O_NONBLOCK);

    if (res == -1) {
        return false;
    }

    return true;
}

void SafeMakeNonblocking(int fd)
{
    if (!TryMakeNonblocking(fd)) {
        THROW_ERROR_EXCEPTION("Failed to set nonblocking mode for descriptor %v", fd)
            << TError::FromSystem();
    }
}

bool TrySetUid(int uid)
{
#ifdef _linux_
    // NB(psushin): setting real uid is really important, e.g. for acceess() call.
    if (setresuid(uid, uid, uid) != 0) {
        return false;
    }
#else
    if (setuid(uid) != 0) {
        return false;
    }
#endif

    return true;
}

void SafeSetUid(int uid)
{
    if (!TrySetUid(uid)) {
        THROW_ERROR_EXCEPTION("Failed to set uid to %v", uid)
            << TError::FromSystem();
    }
}

TString SafeGetUsernameByUid(int uid)
{
    int bufferSize = ::sysconf(_SC_GETPW_R_SIZE_MAX);
    if (bufferSize < 0) {
        THROW_ERROR_EXCEPTION("Failed to get username, sysconf(_SC_GETPW_R_SIZE_MAX) failed")
            << TError::FromSystem();
    }
    char buffer[bufferSize];
    struct passwd pwd, * pwdptr = nullptr;
    int result = getpwuid_r(uid, &pwd, buffer, bufferSize, &pwdptr);
    if (result != 0 || pwdptr == nullptr) {
        // Return #uid in case of absent uid in the system.
        return "#" + ToString(uid);
    }
    return pwdptr->pw_name;
}

#else

bool TryClose(int /* fd */, bool /* ignoreBadFD */)
{
    YT_UNIMPLEMENTED();
}

void SafeClose(int /* fd */, bool /* ignoreBadFD */)
{
    YT_UNIMPLEMENTED();
}

bool TryDup2(int /* oldFD */, int /* newFD */)
{
    YT_UNIMPLEMENTED();
}

void SafeDup2(int /* oldFD */, int /* newFD */)
{
    YT_UNIMPLEMENTED();
}

void SafeSetCloexec(int /* fd */)
{
    YT_UNIMPLEMENTED();
}

bool TryExecve(const char /* *path */, const char* /* argv[] */, const char* /* env[] */)
{
    YT_UNIMPLEMENTED();
}

TError StatusToError(int /* status */)
{
    YT_UNIMPLEMENTED();
}

void CloseAllDescriptors()
{
    YT_UNIMPLEMENTED();
}

void SafePipe(int /* fd */ [2])
{
    YT_UNIMPLEMENTED();
}

int SafeDup(int /* fd */)
{
    YT_UNIMPLEMENTED();
}

void SafeOpenPty(int* /* masterFD */, int* /* slaveFD */, int /* height */, int /* width */)
{
    YT_UNIMPLEMENTED();
}

void SafeLoginTty(int /* slaveFD */)
{
    YT_UNIMPLEMENTED();
}

void SafeSetTtyWindowSize(int /* slaveFD */, int /* height */, int /* width */)
{
    YT_UNIMPLEMENTED();
}

bool TryMakeNonblocking(int /* fd */)
{
    YT_UNIMPLEMENTED();
}

void SafeMakeNonblocking(int /* fd */)
{
    YT_UNIMPLEMENTED();
}

void SafeSetUid(int /* uid */)
{
    YT_UNIMPLEMENTED();
}

TString SafeGetUsernameByUid(int /* uid */)
{
    YT_UNIMPLEMENTED();
}
#endif

void CloseAllDescriptors(const std::vector<int>& exceptFor)
{
#ifdef _linux_
    auto* dirStream = ::opendir("/proc/self/fd");
    YT_VERIFY(dirStream != nullptr);

    int dirFD = ::dirfd(dirStream);
    YT_VERIFY(dirFD >= 0);

    std::vector<int> fds;
    dirent* ep;
    while ((ep = ::readdir(dirStream)) != nullptr) {
        char* begin = ep->d_name;
        char* end = nullptr;
        int fd = static_cast<int>(strtol(begin, &end, 10));
        if (begin == end ||
            fd == dirFD ||
            (std::find(exceptFor.begin(), exceptFor.end(), fd) != exceptFor.end()))
        {
            continue;
        }
        fds.push_back(fd);
    }

    YT_VERIFY(::closedir(dirStream) == 0);

    bool ignoreBadFD = true;
    for (int fd : fds) {
        YT_VERIFY(TryClose(fd, ignoreBadFD));
    }
#endif
}

void SafeCreateStderrFile(TString fileName)
{
#ifdef _unix_
    if (freopen(fileName.data(), "a", stderr) == nullptr) {
        auto lastError = TError::FromSystem();
        THROW_ERROR_EXCEPTION("Stderr redirection failed")
            << lastError;
    }
#endif
}

bool HasRootPermissions()
{
#ifdef _unix_
    uid_t ruid, euid, suid;
#ifdef _linux_
    YT_VERIFY(getresuid(&ruid, &euid, &suid) == 0);
#else
    ruid = getuid();
    euid = geteuid();
    setuid(0);
    suid = getuid();
    YT_VERIFY(seteuid(euid) == 0);
    YT_VERIFY(setruid(ruid) == 0);
#endif
    return suid == 0;
#else // not _unix_
    return false;
#endif
}

TNetworkInterfaceStatisticsMap GetNetworkInterfaceStatistics()
{
#ifdef _linux_
    // According to https://www.kernel.org/doc/Documentation/filesystems/proc.txt,
    // using /proc/net/dev is a stable (and seemingly easiest, despite being nasty)
    // way to access per-interface network statistics.

    TFileInput procNetDev("/proc/net/dev");
    // First two lines are header.
    Y_UNUSED(procNetDev.ReadLine());
    Y_UNUSED(procNetDev.ReadLine());
    TNetworkInterfaceStatisticsMap interfaceToStatistics;
    for (TString line; procNetDev.ReadLine(line) != 0; ) {
        TNetworkInterfaceStatistics statistics;
        TVector<TString> lineParts = StringSplitter(line).SplitBySet(": ").SkipEmpty();
        YT_VERIFY(lineParts.size() == 1 + sizeof(TNetworkInterfaceStatistics) / sizeof(ui64));
        auto interfaceName = lineParts[0];

        int index = 1;
#define XX(field) statistics.field = FromString<ui64>(lineParts[index++])
        XX(Rx.Bytes);
        XX(Rx.Packets);
        XX(Rx.Errs);
        XX(Rx.Drop);
        XX(Rx.Fifo);
        XX(Rx.Frame);
        XX(Rx.Compressed);
        XX(Rx.Multicast);
        XX(Tx.Bytes);
        XX(Tx.Packets);
        XX(Tx.Errs);
        XX(Tx.Drop);
        XX(Tx.Fifo);
        XX(Tx.Colls);
        XX(Tx.Carrier);
        XX(Tx.Compressed);
#undef XX
        // NB: data is racy; duplicates are possible; just deal with it.
        interfaceToStatistics.emplace(interfaceName, statistics);
    }
    return interfaceToStatistics;
#else
    return {};
#endif
}

void SendSignal(const std::vector<int>& pids, const TString& signalName)
{
    ValidateSignalName(signalName);
    auto sig = FindSignalIdBySignalName(signalName);
    for (int pid : pids) {
        if (kill(pid, *sig) != 0 && errno != ESRCH) {
            THROW_ERROR_EXCEPTION("Unable to kill process %d", pid)
                << TError::FromSystem();
        }
    }
}

std::optional<int> FindSignalIdBySignalName(const TString& signalName)
{
    static const THashMap<TString, int> SignalNameToNumber{
        { "SIGHUP",  SIGHUP },
        { "SIGINT",  SIGINT },
        { "SIGALRM", SIGALRM },
        { "SIGKILL", SIGKILL },
        { "SIGTERM", SIGTERM },
        { "SIGUSR1", SIGUSR1 },
        { "SIGUSR2", SIGUSR2 },
        { "SIGURG", SIGURG },
    };

    auto it = SignalNameToNumber.find(signalName);
    return it == SignalNameToNumber.end() ? std::nullopt : std::make_optional(it->second);
}

void ValidateSignalName(const TString& signalName)
{
    auto signal = FindSignalIdBySignalName(signalName);
    if (!signal) {
        THROW_ERROR_EXCEPTION("Unsupported signal name %Qv", signalName);
    }
}

////////////////////////////////////////////////////////////////////////////////

TMemoryMappingStatistics& TMemoryMappingStatistics::operator+=(const TMemoryMappingStatistics& rhs)
{
    Size += rhs.Size;
    KernelPageSize += rhs.KernelPageSize;
    MMUPageSize += rhs.MMUPageSize;
    Rss += rhs.Rss;
    Pss += rhs.Pss;
    SharedClean += rhs.SharedClean;
    SharedDirty += rhs.SharedDirty;
    PrivateClean += rhs.PrivateClean;
    PrivateDirty += rhs.PrivateDirty;
    Referenced += rhs.Referenced;
    Anonymous += rhs.Anonymous;
    LazyFree += rhs.LazyFree;
    AnonHugePages += rhs.AnonHugePages;
    ShmemPmdMapped += rhs.ShmemPmdMapped;
    SharedHugetlb += rhs.SharedHugetlb;
    PrivateHugetlb += rhs.PrivateHugetlb;
    Swap += rhs.Swap;
    SwapPss += rhs.SwapPss;
    Locked += rhs.Locked;

    return *this;
}

TMemoryMappingStatistics operator+(TMemoryMappingStatistics lhs, const TMemoryMappingStatistics& rhs)
{
    lhs += rhs;
    return lhs;
}

std::vector<TMemoryMapping> ParseMemoryMappings(const TString& rawSMaps)
{
    auto parseMemoryAmount = [] (const TString& strValue, const TString& unit) {
        auto value = FromString<ui64>(strValue);
        if (unit == "kB") {
            return value * 1_KB;
        } else {
            YT_VERIFY(false);
        }
    };

    std::vector<TMemoryMapping> memoryMappings;
    for (const auto& token : StringSplitter(rawSMaps).Split('\n')) {
        auto line = token.Token();
        if (line.empty()) {
            continue;
        }

        // TODO(gritukan): Remove it when smaps tracker will be more stable.
        auto verify = [&] (bool condition) {
            if (!condition) {
                Cerr << "Failed to parse smaps: " << rawSMaps << Endl;
                Cerr << "Failed line: " << line << Endl;
                YT_LOG_ERROR("Failed to parse smaps (SMaps: %v)", rawSMaps);
                YT_LOG_ERROR("Failed line (Line: %v)", line);
                YT_VERIFY(false);
            }
        };

        std::vector<TString> words;
        StringSplitter(line).SplitBySet(" \t").SkipEmpty().Collect(&words);

        // Memory mapping description starts with boundary addresses which consists of lowercase
        // letters and digits. Memory mapping properties descriptions starts with uppercase letter.
        if (std::isupper(line[0])) {
            auto property = words[0];
            verify(property.back() == ':');
            property.pop_back();

            verify(!memoryMappings.empty());
            auto& mapping = memoryMappings.back();
            auto& statistics = mapping.Statistics;

            if (property == "Size") {
                verify(words.size() == 3);
                statistics.Size = parseMemoryAmount(words[1], words[2]);
            } else if (property == "KernelPageSize") {
                verify(words.size() == 3);
                statistics.KernelPageSize = parseMemoryAmount(words[1], words[2]);
            } else if (property == "MMUPageSize") {
                verify(words.size() == 3);
                statistics.MMUPageSize = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Rss") {
                verify(words.size() == 3);
                statistics.Rss = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Pss") {
                verify(words.size() == 3);
                statistics.Pss = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Shared_Clean") {
                verify(words.size() == 3);
                statistics.SharedClean = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Shared_Dirty") {
                verify(words.size() == 3);
                statistics.SharedDirty = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Private_Clean") {
                verify(words.size() == 3);
                statistics.PrivateClean = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Private_Dirty") {
                verify(words.size() == 3);
                statistics.PrivateDirty = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Referenced") {
                verify(words.size() == 3);
                statistics.Referenced = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Anonymous") {
                verify(words.size() == 3);
                statistics.Anonymous = parseMemoryAmount(words[1], words[2]);
            } else if (property == "LazyFree") {
                verify(words.size() == 3);
                statistics.LazyFree = parseMemoryAmount(words[1], words[2]);
            } else if (property == "AnonHugePages") {
                verify(words.size() == 3);
                statistics.AnonHugePages = parseMemoryAmount(words[1], words[2]);
            } else if (property == "ShmemPmdMapped") {
                verify(words.size() == 3);
                statistics.ShmemPmdMapped = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Shared_Hugetlb") {
                verify(words.size() == 3);
                statistics.SharedHugetlb = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Private_Hugetlb") {
                verify(words.size() == 3);
                statistics.PrivateHugetlb = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Swap") {
                verify(words.size() == 3);
                statistics.Swap = parseMemoryAmount(words[1], words[2]);
            } else if (property == "SwapPss") {
                verify(words.size() == 3);
                statistics.SwapPss = parseMemoryAmount(words[1], words[2]);
            } else if (property == "Locked") {
                verify(words.size() == 3);
                statistics.Locked = parseMemoryAmount(words[1], words[2]);
            } else if (property == "ProtectionKey") {
                verify(words.size() == 2);
                mapping.ProtectionKey = FromString<ui64>(words[1]);
            } else if (property == "VmFlags") {
                for (const auto& flag : words) {
                    EVMFlag enumFlag;
                    if (TEnumTraits<EVMFlag>::FindValueByLiteral(to_upper(flag), &enumFlag)) {
                        mapping.VMFlags |= enumFlag;
                    } else {
                        // Unknown flag, do not crash.
                    }
                }
            } else {
                // Unknown property, do not crash.
            }
        } else {
            verify(words.size() >= 5);
            TMemoryMapping memoryMapping;
            {
                TStringBuf addressRange = words[0];
                TStringBuf start;
                TStringBuf end;
                verify(addressRange.TrySplit('-', start, end));
                verify(TryIntFromString<16>(start, memoryMapping.Start));
                verify(TryIntFromString<16>(end, memoryMapping.End));
            }
            {
                const auto& permissions = words[1];
                verify(permissions.size() == 4);
                if (permissions[0] == 'r') {
                    memoryMapping.Permissions |= EMemoryMappingPermission::Read;
                } else {
                    verify(permissions[0] == '-');
                }
                if (permissions[1] == 'w') {
                    memoryMapping.Permissions |= EMemoryMappingPermission::Write;
                } else {
                    verify(permissions[1] == '-');
                }
                if (permissions[2] == 'x') {
                    memoryMapping.Permissions |= EMemoryMappingPermission::Execute;
                } else {
                    verify(permissions[2] == '-');
                }
                if (permissions[3] == 'p') {
                    memoryMapping.Permissions |= EMemoryMappingPermission::Private;
                } else {
                    verify(permissions[3] == 's');
                    memoryMapping.Permissions |= EMemoryMappingPermission::Shared;
                }
            }

            verify(TryIntFromString<16>(words[2], memoryMapping.Offset));

            {
                TStringBuf device = words[3];
                TStringBuf majorStr;
                TStringBuf minorStr;
                verify(device.TrySplit(':', majorStr, minorStr));
                ui16 major;
                ui16 minor;
                verify(TryIntFromString<16>(majorStr, major));
                verify(TryIntFromString<16>(minorStr, minor));
                if (major != 0 || minor != 0) {
#ifdef _linux_
                    memoryMapping.DeviceId = makedev(major, minor);
#endif
                }
            }

            if (words[4] != "0") {
                // NB: Decimal number.
                memoryMapping.INode = FromString<ui64>(words[4]);
            }

            if (words.size() >= 6) {
                memoryMapping.Path = words[5];
            }

            memoryMappings.push_back(memoryMapping);
        }
    }

    return memoryMappings;
}

std::vector<TMemoryMapping> GetProcessMemoryMappings(int pid)
{
#ifdef _linux_
    auto rawSMaps = TFileInput{Format("/proc/%v/smaps", pid)}.ReadAll();
    return ParseMemoryMappings(rawSMaps);
#else
    return {};
#endif
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NYT
