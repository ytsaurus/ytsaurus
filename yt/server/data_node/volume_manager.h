#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct IVolume
    : public TRefCounted
{
    virtual const TString& GetPath() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolume)

////////////////////////////////////////////////////////////////////////////////

//! Creates volumes from different layers.
//! Useful for creation and reuse of rootfs volumes.
struct IVolumeManager
    : public TRefCounted
{
    virtual TFuture<IVolumePtr> PrepareVolume(const std::vector<TArtifactKey>& layers) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

IVolumeManagerPtr CreatePortoVolumeManager(
    const TVolumeManagerConfigPtr& config,
    const NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
