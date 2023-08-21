#pragma once

#include "public.h"

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IPersistentResponseKeeper
    : public NRpc::IResponseKeeper
{
public:
    virtual void Evict(TDuration expirationTime, int maxResponseCountPerEvictionPass) = 0;

    virtual void Clear() = 0;

    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPersistentResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

IPersistentResponseKeeperPtr CreatePersistentResponseKeeper(
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
