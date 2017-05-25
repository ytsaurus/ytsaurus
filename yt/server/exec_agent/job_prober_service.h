#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(NCellNode::TBootstrap* jobProxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
