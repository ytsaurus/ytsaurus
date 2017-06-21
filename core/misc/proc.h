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
);

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetPidsByUid(int uid = -1);

//! Gets the resident set size of a process.
/*!
   \note If |pid == -1| then self RSS is returned.
 */
i64 GetProcessRss(int pid = -1);

TString GetProcessName(int pid);
std::vector<TString> GetProcessCommandLine(int pid);

TError StatusToError(int status);
TError ProcessInfoToError(const siginfo_t& processInfo);

void RemoveDirAsRoot(const TString& path);
void RemoveDirContentAsRoot(const TString& path);

bool TryClose(int fd, bool ignoreBadFD = true);
void SafeClose(int fd, bool ignoreBadFD = true);

bool TryDup2(int oldFD, int newFD);
void SafeDup2(int oldFD, int newFD);

void SafeSetCloexec(int fd);

bool TryExecve(const char* path, const char* const* argv, const char* const* env);

void CreateStderrFile(TString fileName);

//! Returns a pipe with CLOSE_EXEC flag.
void SafePipe(int fd[2]);

int SafeDup(int fd);

//! Returns a pty with CLOSE_EXEC flag on master channel.
void SafeOpenPty(int* masterFD, int* slaveFD, int height, int width);
void SafeLoginTty(int fd);
void SafeSetTtyWindowSize(int slaveFD, int height, int width);

bool TryMakeNonblocking(int fd);
void SafeMakeNonblocking(int fd);

void SafeSetUid(int uid);

TString SafeGetUsernameByUid(int uid);

void SetPermissions(const TString& path, int permissions);
void SetPermissions(int fd, int permissions);

void SetUid(int uid);

void CloseAllDescriptors(const std::vector<int>& exceptFor = std::vector<int>());

//! Return true iff ytserver was started with root permissions (e.g. via sudo or with suid bit).
bool HasRootPermissions();

////////////////////////////////////////////////////////////////////////////////

struct TRemoveDirAsRootTool
{
    void operator()(const TString& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TKillAllByUidTool
{
    void operator()(int uid) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveDirContentAsRootTool
{
    void operator()(const TString& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

class TMountTmpfsConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Path;
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

DEFINE_REFCOUNTED_TYPE(TMountTmpfsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMountTmpfsAsRootTool
{
    void operator()(TMountTmpfsConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

class TUmountConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Path;
    bool Detach;

    TUmountConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("detach", Detach);
    }
};

DEFINE_REFCOUNTED_TYPE(TUmountConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUmountAsRootTool
{
    void operator()(TUmountConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class F,  class... Args>
auto HandleEintr(F f, Args&&... args) -> decltype(f(args...));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROC_INL_H_
#include "proc-inl.h"
#undef PROC_INL_H_
