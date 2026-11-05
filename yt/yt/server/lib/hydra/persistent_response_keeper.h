#pragma once

#include "public.h"

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IPersistentResponseKeeper
    : public NRpc::IResponseKeeper
{
    // I'd like to move it elsewhere, but I don't know where yet.
    virtual std::optional<i64> GetGroundUpdateQueueSequenceNumber(NRpc::TMutationId mutationId) = 0;

    virtual void Evict(TDuration expirationTime, int maxResponseCountPerEvictionPass, i64 maxResponsesSpace) = 0;

    virtual void Clear() = 0;

    virtual void Save(TSaveContext& context) const = 0;
    // COMPAT(aleksandra-zh)
    virtual void Load(TLoadContext& context, bool loadGroundUpdateSequenceNumbers) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPersistentResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

IPersistentResponseKeeperPtr CreatePersistentResponseKeeper(
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
