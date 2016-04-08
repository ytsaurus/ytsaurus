#pragma once

#include "common.h"

#include <yt/core/misc/error.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetPidsByUid(int uid);

//! Gets the resident set size of a process.
/*!
   \note If |pid == -1| then self RSS is returned.
 */
i64 GetProcessRss(int pid = -1);

Stroka GetProcessName(int pid);
std::vector<Stroka> GetProcessCommandLine(int pid);

TError StatusToError(int status);

void RemoveDirAsRoot(const Stroka& path);

bool TryClose(int fd, bool ignoreBadFD = true);
void SafeClose(int fd, bool ignoreBadFD = true);

bool TryDup2(int oldFD, int newFD);
void SafeDup2(int oldFD, int newFD);

void SafeSetCloexec(int fd);

bool TryExecve(const char* path, const char* const* argv, const char* const* env);

void CreateStderrFile(Stroka fileName);

// Returns a pipe with CLOSE_EXEC flag.
void SafePipe(int fd[2]);

int SafeDup(int fd);

// Returns a pty with CLOSE_EXEC flag on master channel.
void SafeOpenPty(int* masterFD, int* slaveFD, int height, int width);
void SafeLoginTty(int fd);
void SafeSetTtyWindowSize(int slaveFD, i32 height, i32 width);

bool TryMakeNonblocking(int fd);
void SafeMakeNonblocking(int fd);

void SafeSetUid(int uid);

Stroka SafeGetUsernameByUid(int uid);

void SetPermissions(int fd, int permissions);

void CloseAllDescriptors(const std::vector<int>& exceptFor = std::vector<int>());

DEFINE_ENUM(EProcessErrorCode,
    ((NonZeroExitCode)      (10000))
    ((Signal)               (10001))
);

////////////////////////////////////////////////////////////////////////////////

struct TRemoveDirAsRootTool
{
    void operator()(const Stroka& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

class TMountTmpfsConfig
    : public NYTree::TYsonSerializable
{
public:
    Stroka Path;
    int UserId;
    i64 Size;

    TMountTmpfsConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("user_id", UserId)
            .GreaterThanOrEqual(0);
        RegisterParameter("size", Size)
            .GreaterThanOrEqual(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TMountTmpfsConfig);

////////////////////////////////////////////////////////////////////////////////

struct TMountTmpfsAsRootTool
{
    void operator()(TMountTmpfsConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TUmountAsRootTool
{
    void operator()(const Stroka& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class F,  class... Args>
auto HandleEintr(F f, Args&&... args) -> decltype(f(args...));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROC_INL_H_
#include "proc-inl.h"
#undef PROC_INL_H_
