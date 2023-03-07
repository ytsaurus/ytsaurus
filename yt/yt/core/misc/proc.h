#pragma once

#include "common.h"

#include <yt/core/misc/error.h>

#include <yt/core/ytree/yson_serializable.h>

#include <errno.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! NYT::TError::FromSystem adds this value to a system errno. The enum
//! below lists several errno's that are used in our code.
const int LinuxErrorCodeBase = 4200;

DEFINE_ENUM(ELinuxErrorCode,
    ((NOSPC)((LinuxErrorCodeBase + ENOSPC)))
    ((NOENT)((LinuxErrorCodeBase + ENOENT)))
);

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetPidsByUid(int uid = -1);
std::vector<int> GetPidsUnderParent(int targetPid);

//! Gets the resident set size of a process.
/*!
   \note If |pid == -1| then self RSS is returned.
 */

struct TMemoryUsage
{
    ui64 Rss;
    ui64 Shared;
};

TMemoryUsage GetProcessMemoryUsage(int pid = -1);

THashMap<TString, i64> GetVmstat();

ui64 GetProcessCumulativeMajorPageFaults(int pid = -1);

size_t GetCurrentProcessId();

size_t GetCurrentThreadId();

void ChownChmodDirectoriesRecursively(
    const TString& path,
    const std::optional<uid_t>& userId,
    const std::optional<int>& permissions);

void SetThreadPriority(int tid, int priority);

TString GetProcessName(int pid);
std::vector<TString> GetProcessCommandLine(int pid);

TError StatusToError(int status);
TError ProcessInfoToError(const siginfo_t& processInfo);

bool TryClose(int fd, bool ignoreBadFD = true);
void SafeClose(int fd, bool ignoreBadFD = true);

bool TryDup2(int oldFD, int newFD);
void SafeDup2(int oldFD, int newFD);

void SafeSetCloexec(int fd);

bool TryExecve(const char* path, const char* const* argv, const char* const* env);

void SafeCreateStderrFile(TString fileName);

//! Returns a pipe with CLOSE_EXEC flag.
void SafePipe(int fd[2]);

int SafeDup(int fd);

//! Returns a pty with CLOSE_EXEC flag on master channel.
void SafeOpenPty(int* masterFD, int* slaveFD, int height, int width);
void SafeLoginTty(int fd);
void SafeSetTtyWindowSize(int slaveFD, int height, int width);

bool TryMakeNonblocking(int fd);
void SafeMakeNonblocking(int fd);

bool TrySetUid(int uid);
void SafeSetUid(int uid);

TString SafeGetUsernameByUid(int uid);

void SetUid(int uid);

void CloseAllDescriptors(const std::vector<int>& exceptFor = std::vector<int>());

//! Return true iff ytserver was started with root permissions (e.g. via sudo or with suid bit).
bool HasRootPermissions();

struct TNetworkInterfaceStatistics
{
    struct TReceiveStatistics
    {
        ui64 Bytes = 0;
        ui64 Packets = 0;
        ui64 Errs = 0;
        ui64 Drop = 0;
        ui64 Fifo = 0;
        ui64 Frame = 0;
        ui64 Compressed = 0;
        ui64 Multicast = 0;
    };
    struct TTransmitStatistics
    {
        ui64 Bytes = 0;
        ui64 Packets = 0;
        ui64 Errs = 0;
        ui64 Drop = 0;
        ui64 Fifo = 0;
        ui64 Colls = 0;
        ui64 Carrier = 0;
        ui64 Compressed = 0;
    };

    TReceiveStatistics Rx;
    TTransmitStatistics Tx;
};

using TNetworkInterfaceStatisticsMap = THashMap<TString, TNetworkInterfaceStatistics>;
//! Returns a mapping from interface name to network statistics.
TNetworkInterfaceStatisticsMap GetNetworkInterfaceStatistics();

void SendSignal(const std::vector<int>& pids, const TString& signalName);
std::optional<int> FindSignalIdBySignalName(const TString& signalName);
void ValidateSignalName(const TString& signalName);

////////////////////////////////////////////////////////////////////////////////

template <class F,  class... Args>
auto HandleEintr(F f, Args&&... args) -> decltype(f(args...));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROC_INL_H_
#include "proc-inl.h"
#undef PROC_INL_H_
