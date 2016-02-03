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

bool TryExecve(const char* path, const char* const* argv, const char* const* env);

void CreateStderrFile(Stroka fileName);

// Returns a pipe with CLOSE_EXEC flag.
void SafePipe(int fd[2]);

bool TryMakeNonblocking(int fd);
void SafeMakeNonblocking(int fd);

void SafeSetUid(int uid);

void SetPermissions(int fd, int permissions);

void CloseAllDescriptors(const std::vector<int>& exceptFor = std::vector<int>());

DEFINE_ENUM(EExitStatus,
    ((ExitCodeBase)         (10000))

    ((SignalBase)           (11000))
    ((SigTerm)              (11006))
    ((SigKill)              (11009))
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

} // namespace NYT
