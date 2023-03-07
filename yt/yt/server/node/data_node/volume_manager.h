#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct IVolume
    : public virtual TRefCounted
{
    virtual const TString& GetPath() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolume)

////////////////////////////////////////////////////////////////////////////////

//! Creates volumes from different layers.
//! Useful for creation of rootfs volumes.
struct IVolumeManager
    : public virtual TRefCounted
{
    virtual TFuture<IVolumePtr> PrepareVolume(const std::vector<TArtifactKey>& layers) = 0;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

IVolumeManagerPtr CreatePortoVolumeManager(
    TVolumeManagerConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
