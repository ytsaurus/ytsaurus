#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using TVolumeId = TGuid;

////////////////////////////////////////////////////////////////////////////////

struct IVolume
    : public virtual TRefCounted
{
    //! Get unique volume id.
    virtual const TVolumeId& GetId() const = 0;
    //! Get absolute path to volume mount point.
    virtual const std::string& GetPath() const = 0;
    //! Link volume mount point to target.
    virtual TFuture<void> Link(
        TGuid tag,
        const std::string& target) = 0;
    //! Unlink volume from all targets without removing the volume.
    //! Used for reusable volumes that need to be re-linked in the next job.
    virtual TFuture<void> Unlink() = 0;
    //! Remove volume and links where it points to.
    virtual TFuture<void> Remove() = 0;
    //! Check if volume is cached.
    virtual bool IsCached() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolume)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
