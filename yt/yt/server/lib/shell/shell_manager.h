#pragma once

#include "public.h"

#include <yt/yt/library/containers/public.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <optional>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

struct TShellManagerConfig
{
public:
    TString PreparationDir;
    TString WorkingDir;

    std::optional<int> UserId;
    std::optional<int> GroupId;

    std::optional<TString> MessageOfTheDay;
    std::vector<TString> Environment;

    bool EnableJobShellSeccopm;
};

////////////////////////////////////////////////////////////////////////////////

struct IShellManager
    : public virtual TRefCounted
{
    virtual NApi::TPollJobShellResponse PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters) = 0;
    virtual void Terminate(const TError& error) = 0;
    virtual TFuture<void> GracefulShutdown(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShellManager)

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreatePortoShellManager(
    const TShellManagerConfig& config,
    NContainers::IPortoExecutorPtr portoExecutor,
    NContainers::IInstancePtr rootInstance);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
