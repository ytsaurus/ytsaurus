#pragma once

#include <yt/yt/server/node/data_node/bootstrap.h>

NYT::NDataNode::IBootstrapPtr CreateBootstrap(
    NYT::IInvokerPtr invoker,
    NYT::NClusterNode::TClusterNodeBootstrapConfigPtr config,
    NYT::NLogging::TLogger logger,
    NYT::NProfiling::TProfiler profiler);
