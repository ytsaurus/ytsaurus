#pragma once

#include <yt/core/misc/intrusive_ptr.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJobSpecHelper)
DECLARE_REFCOUNTED_STRUCT(IUserJobIOFactory)
DECLARE_REFCOUNTED_CLASS(TUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
