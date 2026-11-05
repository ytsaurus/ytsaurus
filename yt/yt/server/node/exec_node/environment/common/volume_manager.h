#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/volume_manager.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IHostVolumeManager
    : public IVolumeManager
{
    virtual TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        const std::optional<std::string>& sandboxPath,
        const TJobId& jobId,
        const std::vector<TBaseVolumeParamsPtr>& volumes,
        const std::vector<TVolumeMountPtr>& volumeMounts) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHostVolumeManager)

////////////////////////////////////////////////////////////////////////////////

TFuture<IHostVolumeManagerPtr> CreateHostVolumeManager(
    const std::vector<TSlotLocationConfigPtr>& locations,
    IInvokerPtr invoker,
    bool detachedTmpfsUmount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
