#pragma once

#include "slot.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class THostUserSlot
    : public TUserSlotBase
{
public:
    explicit THostUserSlot(TUserSlotContext context);

    TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        TJobId jobId,
        const IVolumePtr& rootVolume,
        const std::vector<TBaseVolumeParamsPtr>& volumeParams,
        const std::vector<TVolumeMountPtr>& volumeMounts);
};

DEFINE_REFCOUNTED_TYPE(THostUserSlot)

////////////////////////////////////////////////////////////////////////////////

IUserSlotPtr CreateHostUserSlot(TUserSlotContext context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
