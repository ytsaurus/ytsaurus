#pragma once

#include "public.h"

#include <yt/server/scheduler/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobSpecService(NScheduler::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
