#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/actions/signal.h>

#include <vector>
#include <array>
#include <optional>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IRandomFileProvider
    : public TRefCounted
{
    struct TFileInfo
    {
        TString Path;
        i64 DiskSpace = 0;
    };

    virtual std::optional<TFileInfo> GetRandomExistingFile() = 0;
};

DEFINE_REFCOUNTED_TYPE(IRandomFileProvider);

////////////////////////////////////////////////////////////////////////////////

struct IGentleLoader
    : public TRefCounted
{
    // TODO(capone212): pass packets histogram here.
    virtual void Start() = 0;
    virtual void Stop() = 0;

    //! Raised when disk overload is detected.
    DECLARE_INTERFACE_SIGNAL(void(i64 /*currentIOPS*/), Congested);
};

DEFINE_REFCOUNTED_TYPE(IGentleLoader);

////////////////////////////////////////////////////////////////////////////////

IGentleLoaderPtr CreateGentleLoader(
    const TGentleLoaderConfigPtr& config,
    const TString& locationRoot,
    IIOEngineWorkloadModelPtr engine,
    IRandomFileProviderPtr fileProvider,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NIO
