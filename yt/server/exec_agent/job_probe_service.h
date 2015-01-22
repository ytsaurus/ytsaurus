#pragma once

#include "public.h"

#include <server/cell_node/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProbeService(NCellNode::TBootstrap* jobProxy);

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT