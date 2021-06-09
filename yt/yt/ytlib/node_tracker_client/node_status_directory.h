#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct INodeStatusDirectory
    : public virtual TRefCounted
{
    virtual void UpdateSuspicionMarkTime(
        const TString& nodeAddress,
        bool suspicious,
        std::optional<TInstant> previousMarkTime) = 0;

    virtual std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TString>& nodeAddresses) const = 0;

    virtual bool ShouldMarkNodeSuspicious(const TError& error) const = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeStatusDirectory)

////////////////////////////////////////////////////////////////////////////////

INodeStatusDirectoryPtr CreateTrivialNodeStatusDirectory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
