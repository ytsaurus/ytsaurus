#pragma once

#include "config.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TNodeInfoPtr GetNodeInfo(const TFlowNodeConfigPtr& config, const NLogging::TLogger& logger);

//! Extracts the snapshot id from the porto container name of the current
//! process ("..._sn_<ID>_start" workload containers of snapshot stages).
//! Returns null outside a snapshot-stage box.
std::optional<std::string> TryExtractDeploySnapshotId(const std::vector<TProcessCgroup>& cgroups);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
