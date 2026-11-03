#pragma once

#include <yt/yt/server/node/exec_node/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJobFilesystem;

DECLARE_REFCOUNTED_STRUCT(IHostVolumeManager)
DECLARE_REFCOUNTED_CLASS(TUserSlotBase)
DECLARE_REFCOUNTED_CLASS(THostUserSlot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
