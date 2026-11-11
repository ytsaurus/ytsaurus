#pragma once

#include <yt/yql/providers/dq/actors/yt/resource_manager.h>

#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NYql {

class TLiteralRequestsHardReserveSettings {
public:
    struct TClusterSettings {
        ui32 Percent = 0;
        ui64 MinSlots = 0;

        ui64 CalculateReserve(ui64 capacity) const;
    };

    explicit TLiteralRequestsHardReserveSettings(const TVector<TResourceManagerOptions>& backendOptions);

    bool IsEnabled() const { return !Clusters.empty(); }
    const THashMap<TString, TClusterSettings>& GetClusters() const { return Clusters; }
    const TClusterSettings* Find(const TString& clusterName) const;

private:
    THashMap<TString, TClusterSettings> Clusters;
};

} // namespace NYql
