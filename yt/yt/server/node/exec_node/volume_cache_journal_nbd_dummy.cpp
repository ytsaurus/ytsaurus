#include "environment/porto/volume_cache.h"

#include "volume_options.h"

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TFuture<NNbd::IBlockDevicePtr> TNbdVolumeFactory::CreateRWJournalNbdDevice(
    TGuid /*tag*/,
    const TVolumePreparationContext& /*context*/,
    TPrepareRWNbdVolumeOptions /*options*/)
{
    return MakeFuture<NNbd::IBlockDevicePtr>(TError(
        NExecNode::EErrorCode::InvalidNbdDiskRequest,
        "Journal NBD disks are not supported in this build"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
