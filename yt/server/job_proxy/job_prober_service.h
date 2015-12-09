#pragma once

#include "public.h"
#include "job_proxy.h"

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(TJobProxy* jobProxy);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT