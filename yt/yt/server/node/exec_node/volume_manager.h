#pragma once

#include "public.h"

#include "chunk_cache.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

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
    virtual TFuture<IVolumePtr> PrepareVolume(
        const std::vector<NDataNode::TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions) = 0;

    virtual bool IsLayerCached(const NDataNode::TArtifactKey& artifactKey) const = 0;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

IVolumeManagerPtr CreatePortoVolumeManager(
    NDataNode::TVolumeManagerConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
