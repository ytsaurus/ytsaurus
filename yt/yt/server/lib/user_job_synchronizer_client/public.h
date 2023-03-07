#pragma once

#include <yt/core/misc/intrusive_ptr.h>

namespace NYT::NUserJobSynchronizerClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUserJobSynchronizerConnectionConfig)

DECLARE_REFCOUNTED_STRUCT(IUserJobSynchronizer)
DECLARE_REFCOUNTED_STRUCT(IUserJobSynchronizerClient)

DECLARE_REFCOUNTED_CLASS(TUserJobSynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobSynchronizerClient
