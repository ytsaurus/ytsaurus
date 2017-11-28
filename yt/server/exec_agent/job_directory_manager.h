#pragma once

#include "public.h"

#include <yt/server/data_node/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct TJobDirectoryProperties
{
    TNullable<i64> DiskSpaceLimit;
    TNullable<i64> InodeLimit;
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
};

DEFINE_REFCOUNTED_TYPE(IJobDirectoryManager)

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

IJobDirectoryManagerPtr CreatePortoJobDirectoryManager(
    NDataNode::TVolumeManagerConfigPtr config,
    const TString& path);

#endif

IJobDirectoryManagerPtr CreateSimpleJobDirectoryManager(
    IInvokerPtr invoker,
    const TString& path,
    bool detachedTmpfsUmount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT