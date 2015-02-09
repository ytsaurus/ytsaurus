#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <server/cell_scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(NCellScheduler::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT