#pragma once

#include "public.h"

#include <yt/server/containers/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/fs.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct IMounter
    : public TRefCounted
{
    virtual std::vector<NFS::TMountPoint> GetMountPoints() const = 0;
    virtual void Mount(TMountTmpfsConfigPtr config) = 0;
    virtual void Umount(TUmountConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMounter)

IMounterPtr CreateSimpleMounter(const IInvokerPtr invoker);
IMounterPtr CreatePortoMounter(TCallback<NContainers::IInstancePtr()> instanceProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
