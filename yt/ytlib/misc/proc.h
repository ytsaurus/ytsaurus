#pragma once

#include "common.h"

#include <ytlib/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetPidsByUid(int uid);

//! Gets the resident set size of a process.
/*!
   \note If |pid == -1| then self RSS is returned.
 */
i64 GetProcessRss(int pid = -1);

void KilallByUid(int uid);

TError StatusToError(int status);

void RemoveDirAsRoot(const Stroka& path);

void SafeClose(int fd, bool ignoreInvalidFd = false);

void CloseAllDescriptors();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
