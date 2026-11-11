#include "literal_requests_hard_reserve.h"

#include <util/generic/yexception.h>

namespace NYql {

ui64 TLiteralRequestsHardReserveSettings::TClusterSettings::CalculateReserve(ui64 capacity) const
{
    const ui64 wholeHundreds = capacity / 100;
    const ui64 remainder = capacity % 100;
    const ui64 percentReserve = wholeHundreds * Percent + (remainder * Percent + 99) / 100;
    return Min(capacity, Max(MinSlots, percentReserve));
}

TLiteralRequestsHardReserveSettings::TLiteralRequestsHardReserveSettings(
    const TVector<TResourceManagerOptions>& backendOptions)
{
    THashMap<TString, ui32> backendCountByCluster;
    for (const auto& options : backendOptions) {
        const TString clusterName = options.AnnounceClusterName.GetOrElse(
            options.YtBackend.GetClusterName());
        ++backendCountByCluster[clusterName];
    }

    for (const auto& options : backendOptions) {
        const auto& backend = options.YtBackend;
        const ui32 percent = backend.GetDqLiteralReservePercent();
        const ui64 minSlots = backend.GetDqLiteralReserveMinSlots();
        if (percent == 0 && minSlots == 0) {
            continue;
        }

        const TString clusterName = options.AnnounceClusterName.GetOrElse(
            backend.GetClusterName());
        if (clusterName.empty()) {
            ythrow yexception() << "DQ literal requests hard reserve requires a non-empty backend cluster name";
        }
        if (percent > 100) {
            ythrow yexception()
                << "DqLiteralReservePercent must be in range [0, 100] for backend "
                << clusterName << ", got " << percent;
        }
        if (backendCountByCluster.at(clusterName) != 1) {
            ythrow yexception()
                << "DQ literal requests hard reserve is ambiguous for worker cluster "
                << clusterName << ": multiple YT backends announce the same name";
        }

        Clusters.emplace(clusterName, TClusterSettings{
            .Percent = percent,
            .MinSlots = minSlots,
        });
    }
}

const TLiteralRequestsHardReserveSettings::TClusterSettings*
TLiteralRequestsHardReserveSettings::Find(const TString& clusterName) const
{
    return Clusters.FindPtr(clusterName);
}

} // namespace NYql
