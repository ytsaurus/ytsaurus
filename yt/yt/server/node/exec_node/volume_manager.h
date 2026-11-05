#pragma once

#include "preparation_options.h"
#include "public.h"
#include "volume.h"
#include "volume_artifact.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls the node-wide volume service lifecycle and resource cleanup.
struct IVolumeManager
    : public virtual TRefCounted
{
    virtual void InitializeSlot(TNonNullPtr<TSlotLocation> location, int slotIndex) = 0;

    //! Prepares this manager for replacement during job environment resurrection,
    //! preserving resources that the new manager can reuse.
    virtual void PrepareForReplacement() const = 0;

    virtual void BuildOrchid(NYTree::TFluentAny fluent) const = 0;

    virtual TFuture<void> GetVolumeReleaseEvent() = 0;

    virtual TFuture<void> Disable(const TError& reason) = 0;

    virtual bool IsEnabled() const = 0;

    virtual TError GetHealthCheckError() const = 0;

    virtual void OnDynamicConfigChanged(
        const TVolumeManagerDynamicConfigPtr& oldConfig,
        const TVolumeManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
