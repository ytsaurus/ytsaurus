#pragma once

#include "common.h"

#include <ytlib/misc/error.h>

#ifdef _linux_
    #include <sys/resource.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Gets resident set size of a process.
/*!
   \note If pid == -1 self RSS is returned
 */
i64 GetProcessRss(int pid = -1);
i64 GetUserRss(int uid);

void KillallByUser(int uid);

TError StatusToError(int status);

void RemoveDirAsRoot(const Stroka& path);

void SafeClose(int fd, bool ignoreInvalidFd = false);

void CloseAllDescriptors();

int SetMemoryLimit(rlim_t memoryLimit);

int Spawn(const char* path,
          std::initializer_list<const char*> arguments,
          const std::vector<int>& fileIdsToClose);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
