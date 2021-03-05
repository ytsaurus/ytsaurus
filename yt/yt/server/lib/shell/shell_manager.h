#pragma once

#include "public.h"

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/intrusive_ptr.h>
#include <yt/yt/core/misc/optional.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

struct IShellManager
    : public virtual TRefCounted
{
    virtual NYson::TYsonString PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters) = 0;
    virtual void Terminate(const TError& error) = 0;
    virtual TFuture<void> GracefulShutdown(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShellManager)

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreateShellManager(
    NContainers::IPortoExecutorPtr portoExecutor,
    NContainers::IInstancePtr rootInstnace,
    const TString& preparationDir,
    const TString& workingDir,
    std::optional<int> userId,
    std::optional<int> groupId,
    std::optional<TString> messageOfTheDay,
    std::vector<TString> environment);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
