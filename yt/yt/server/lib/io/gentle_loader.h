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

DEFINE_REFCOUNTED_TYPE(IRandomFileProvider)

////////////////////////////////////////////////////////////////////////////////

struct IGentleLoader
    : public TRefCounted
{
    virtual void Start(const TRequestSizes& workloadModel) = 0;

    virtual void Stop() = 0;

    //! Raised when disk overload is detected.
    DECLARE_INTERFACE_SIGNAL(void(i64 /*currentIOPS*/), Congested);

    //! Raised on new congestion detection round.
    DECLARE_INTERFACE_SIGNAL(void(i64 /*currentIOPS*/), ProbesRoundFinished);
};

DEFINE_REFCOUNTED_TYPE(IGentleLoader)

////////////////////////////////////////////////////////////////////////////////

IGentleLoaderPtr CreateGentleLoader(
    TGentleLoaderConfigPtr config,
    TString locationRoot,
    IIOEngineWorkloadModelPtr engine,
    IRandomFileProviderPtr fileProvider,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

class ILoadAdjuster
    : public TRefCounted
{
public:
    virtual bool ShouldSkipRead(i64 size) = 0;
    virtual bool ShouldSkipWrite(i64 size) = 0;
    virtual void ResetStatistics() = 0;

    // Method for testing purposes.
    virtual double GetReadProbability() const = 0;
    virtual double GetWriteProbability() const = 0;
};

DECLARE_REFCOUNTED_CLASS(ILoadAdjuster)
DEFINE_REFCOUNTED_TYPE(ILoadAdjuster)

ILoadAdjusterPtr CreateLoadAdjuster(
    TGentleLoaderConfigPtr config,
    IIOEngineWorkloadModelPtr engine,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
