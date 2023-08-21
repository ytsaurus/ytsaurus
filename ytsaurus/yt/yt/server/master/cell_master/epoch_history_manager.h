#pragma once

#include "public.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct IEpochHistoryManager
    : public virtual TRefCounted
{
    virtual std::pair<TInstant, TInstant> GetEstimatedMutationTime(
        NHydra::TVersion version,
        TInstant now) const = 0;

    virtual std::pair<TInstant, TInstant> GetEstimatedCreationTime(
        NObjectClient::TObjectId id,
        TInstant now) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IEpochHistoryManager)

////////////////////////////////////////////////////////////////////////////////

IEpochHistoryManagerPtr CreateEpochHistoryManager(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
