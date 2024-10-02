#pragma once

#include "public.h"

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceSensorsHolder
{
public:
    struct TMethodSensors
    {
        // Time between the request arrival and the moment when it is fully processed.
        NProfiling::TEventTimer TotalTimeCounter;
    };

    TObjectServiceSensorsHolder();

    TMethodSensors* FindOrCreateSelectObjectHistorySensors(
        const TString& methodName,
        const NRpc::TAuthenticationIdentity& identity,
        bool indexUsed);

private:
    using TSelectObjectHistorySensorsKey = std::tuple<NRpc::TAuthenticationIdentity, bool>;
    using TSelectObjectHistorySensorsKeyHash = THash<TSelectObjectHistorySensorsKey>;

    using TSelectObjectHistorySensorsMap = NConcurrency::TSyncMap<
        TSelectObjectHistorySensorsKey,
        TMethodSensors,
        TSelectObjectHistorySensorsKeyHash>;

    NProfiling::TProfiler Profiler_;

    TSelectObjectHistorySensorsMap SelectObjectHistorySensorsMap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
