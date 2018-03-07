#pragma once

#include <yt/core/misc/enum.h>
#include <yt/core/misc/intrusive_ptr.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((MemoryLimitExceeded)     (1200))
    ((MemoryCheckFailed)       (1201))
    ((JobTimeLimitExceeded)    (1202))
    ((UnsupportedJobType)      (1203))
    ((JobNotPrepared)          (1204))
    ((UserJobFailed)           (1205))
);

DECLARE_REFCOUNTED_STRUCT(IJobSpecHelper)
DECLARE_REFCOUNTED_STRUCT(IUserJobIOFactory)
DECLARE_REFCOUNTED_STRUCT(IUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
