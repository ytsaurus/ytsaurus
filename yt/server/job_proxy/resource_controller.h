#pragma once

#include "public.h"

#include <yt/server/containers/public.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/ytree/public.h>
#include <yt/core/misc/process.h>


namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using TCpuStatistics = NCGroup::TCpuAccounting::TStatistics;
using TBlockIOStatistics = NCGroup::TBlockIO::TStatistics;
using TMemoryStatistics = NCGroup::TMemory::TStatistics;

////////////////////////////////////////////////////////////////////////////////

struct IResourceController
    : public TRefCounted
{
    virtual TCpuStatistics GetCpuStatistics() const = 0;
    virtual TBlockIOStatistics GetBlockIOStatistics() const = 0;
    virtual TMemoryStatistics GetMemoryStatistics() const = 0;
    virtual i64 GetMaxMemoryUsage() const = 0;
    virtual TDuration GetBlockIOWatchdogPeriod() const = 0;
    virtual void KillAll() = 0;
    virtual void SetCpuShare(double share) = 0;
    virtual void SetIOThrottle(i64 operations) = 0;
    virtual IResourceControllerPtr CreateSubcontroller(const TString& name) = 0;
    virtual TProcessBasePtr CreateControlledProcess(const TString& path, int uid, const TNullable<TString>& coreDumpHandler) = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceController)

////////////////////////////////////////////////////////////////////////////////

IResourceControllerPtr CreateResourceController(NYTree::INodePtr config, const TNullable<NContainers::TRootFS>& rootFS);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
