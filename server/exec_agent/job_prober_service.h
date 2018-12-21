#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(NCellNode::TBootstrap* jobProxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
