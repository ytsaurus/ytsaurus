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

void SafeClose(int fd);

void CloseAllDescriptors();

DECLARE_ENUM(EExitStatus,
    ((ExitCodeBase)         (10000))

    ((SignalBase)           (11000))
    ((SigTerm)              (11006))
    ((SigKill)              (11009))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
