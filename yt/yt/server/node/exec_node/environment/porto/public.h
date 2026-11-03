#pragma once

#include <yt/yt/server/node/exec_node/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayer)
DECLARE_REFCOUNTED_CLASS(TLayerLocation)
DECLARE_REFCOUNTED_CLASS(TPortoJobFSSecretary)
DECLARE_REFCOUNTED_STRUCT(IPortoVolumeManager)
DECLARE_REFCOUNTED_CLASS(TPortoUserSlot)

DECLARE_REFCOUNTED_CLASS(TSquashFSVolume)
DECLARE_REFCOUNTED_CLASS(TRWNbdVolume)
DECLARE_REFCOUNTED_CLASS(TRONbdVolume)
DECLARE_REFCOUNTED_CLASS(TOverlayVolume)
DECLARE_REFCOUNTED_CLASS(TTmpfsVolume)
DECLARE_REFCOUNTED_CLASS(TLoopVolume)

class TVolumeProfilerCounters;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
