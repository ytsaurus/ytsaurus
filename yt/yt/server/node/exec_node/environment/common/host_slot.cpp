#include "host_slot.h"

#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/volume_artifact.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

THostUserSlot::THostUserSlot(TUserSlotContext context)
    : TUserSlotBase(std::move(context))
{ }

TFuture<std::vector<TVolumeResultPtr>> THostUserSlot::PrepareNonRootVolumes(
    TJobId jobId,
    const IVolumePtr& rootVolume,
    const std::vector<TBaseVolumeParamsPtr>& volumeParams,
    const std::vector<TVolumeMountPtr>& volumeMounts)
{
    for (const auto& volume : volumeParams) {
        YT_VERIFY(volume->LayerArtifactKeys.GetAll().empty());
    }

    return DoPrepareNonRootVolumes(
        rootVolume,
        volumeParams,
        volumeMounts,
        BIND([&] (const std::string& sandboxPath) {
            return static_cast<IHostVolumeManager*>(VolumeManager_.Get())->PrepareNonRootVolumes(
                sandboxPath,
                jobId,
                volumeParams,
                volumeMounts);
        }));
}

IUserSlotPtr CreateHostUserSlot(TUserSlotContext context)
{
    auto invoker = context.Bootstrap->GetJobInvoker();
    return NewWithOffloadedDtor<THostUserSlot>(std::move(invoker), std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
