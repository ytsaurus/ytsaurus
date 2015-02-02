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

TError StatusToError(int status);

void RunCleaner(const Stroka& path);

void RemoveDirAsRoot(const Stroka& path);

bool TryClose(int fd);
void SafeClose(int fd);

bool TryDup2(int oldFd, int newFd);
void SafeDup2(int oldFd, int newFd);

bool TryExecve(const char* path, char* const argv[], char* const env[]);

void SafePipe(int fd[2]);
void SafeMakeNonblocking(int fd);

void SetPermissions(int fd, int permissions);

void CloseAllDescriptors(const std::vector<int>& exceptFor = std::vector<int>());

DEFINE_ENUM(EExitStatus,
    ((ExitCodeBase)         (10000))

    ((SignalBase)           (11000))
    ((SigTerm)              (11006))
    ((SigKill)              (11009))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
