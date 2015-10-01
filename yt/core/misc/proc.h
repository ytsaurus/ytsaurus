#pragma once

#include "common.h"

#include <core/misc/error.h>

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

bool TryExecve(const char* path, char* const argv[], char* const env[]);

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

} // namespace NYT
