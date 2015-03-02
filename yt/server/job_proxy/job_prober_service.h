#pragma once

#include "public.h"
#include "job_proxy.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(TJobProxy* jobProxy);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT