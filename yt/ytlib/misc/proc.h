#pragma once

#include "common.h"

#include <ytlib/misc/error.h>

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

void RemoveDirAsRoot(Stroka path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
