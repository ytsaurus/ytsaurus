#pragma once

#include "public.h"

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TJobDirectoryProperties
{
    std::optional<i64> DiskSpaceLimit;
    std::optional<i64> InodeLimit;
    int UserId;
};

//! Manages directories with quota restrictions for user job sandbox and tmpfs.
struct IJobDirectoryManager
    : public TRefCounted
{
    virtual TFuture<void> ApplyQuota(
        const TString& path,
        const TJobDirectoryProperties& properties) = 0;

    virtual TFuture<void> CreateTmpfsDirectory(
        const TString& path,
        const TJobDirectoryProperties& properties) = 0;

    //! Releases all managed directories with given path prefix.
    virtual TFuture<void> CleanDirectories(const TString& pathPrefix) = 0;

    virtual void OnDynamicConfigChanged(
        const TJobDirectoryManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobDirectoryManager)

#ifdef _linux_

IJobDirectoryManagerPtr CreatePortoJobDirectoryManager(
    NExecNode::TJobDirectoryManagerDynamicConfigPtr config,
    bool enableDiskQuota,
    const TString& path,
    int locationIndex);

#endif

IJobDirectoryManagerPtr CreateSimpleJobDirectoryManager(
    IInvokerPtr invoker,
    const TString& path,
    bool detachedTmpfsUmount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
