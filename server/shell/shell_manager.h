#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/intrusive_ptr.h>
#include <yt/core/misc/nullable.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NShell {

////////////////////////////////////////////////////////////////////////////////

struct IShellManager
    : public virtual TRefCounted
{
    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) = 0;
    virtual void Terminate(const TError& error) = 0;
    virtual TFuture<void> GracefulShutdown(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShellManager)

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreateShellManager(
    const TString& workingDir,
    TNullable<int> userId,
    TNullable<TString> freezerFullPath,
    TNullable<TString> messageOfTheDay,
    std::vector<TString> environment);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
