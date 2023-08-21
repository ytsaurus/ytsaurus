#pragma once

#include <yt/yt/core/misc/intrusive_ptr.h>

namespace NYT::NUserJob {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IUserJobSynchronizer)
DECLARE_REFCOUNTED_STRUCT(IUserJobSynchronizerClient)

DECLARE_REFCOUNTED_CLASS(TUserJobSynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJob
