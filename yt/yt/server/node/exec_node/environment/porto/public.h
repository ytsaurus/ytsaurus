#pragma once

#include <yt/yt/server/node/exec_node/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayer)
DECLARE_REFCOUNTED_CLASS(TLayerLocation)
DECLARE_REFCOUNTED_CLASS(TPortoJobFSSecretary)
DECLARE_REFCOUNTED_STRUCT(IPortoVolumeManager)
DECLARE_REFCOUNTED_CLASS(TPortoUserSlot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
