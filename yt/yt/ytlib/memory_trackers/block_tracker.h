#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/misc/block_tracker.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

IBlockTrackerPtr CreateBlockTracker(INodeMemoryTrackerPtr tracker);

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
