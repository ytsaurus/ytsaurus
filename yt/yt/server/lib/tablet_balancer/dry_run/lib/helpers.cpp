#include "helpers.h"
#include "holders.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTabletBalancer::NDryRun {

using namespace NCypressClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TTabletPtr FindTabletInBundle(const TTabletCellBundlePtr& bundle, TTabletId tabletId)
{
    for (const auto& [tableId, table] : bundle->Tables) {
        for (const auto& tablet : table->Tablets) {
            if (tablet->Id == tabletId) {
                return tablet;
            }
        }
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
