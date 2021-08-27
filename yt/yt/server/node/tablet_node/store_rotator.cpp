#include "store_rotator.h"

#include "bootstrap.h"
#include "private.h"
#include "public.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/lsm/tablet.h>

namespace NYT::NTabletNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TStoreRotator
    : public IStoreRotator
{
public:
    explicit TStoreRotator(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

private:
    IBootstrap* const Bootstrap_;

    virtual void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) override
    {
        THashMap<TCellId, std::vector<NLsm::TTabletPtr>> tabletsByCellId;

        auto scheduleRotation = [] (const auto& tabletManager, const auto& lsmTablet) {
            auto* tablet = tabletManager->FindTablet(lsmTablet->GetId());
            if (!tablet) {
                YT_LOG_DEBUG("Tablet is missing, aborting rotation (TabletId: %v)",
                    lsmTablet->GetId());
                return;
            }

            if (tablet->GetMountRevision() != lsmTablet->GetMountRevision()) {
                YT_LOG_DEBUG("Mount revision mismatch, aborting rotation "
                    "(ExpectedMountRevision: %v, ActualMountRevision: %v)",
                    lsmTablet->GetMountRevision(),
                    tablet->GetMountRevision());
                return;
            }

            auto activeStore = tablet->GetActiveStore();
            auto activeStoreId = activeStore ? activeStore->GetId() : TStoreId{};
            if (lsmTablet->FindActiveStore()->GetId() != activeStoreId) {
                YT_LOG_DEBUG("Active store id mismatch, aborting rotation "
                    "(ExpectedDynamicStoreId: %v, ActualDynamicStoreId: %v)",
                    lsmTablet->FindActiveStore()->GetId(),
                    activeStoreId);
            }

            tabletManager->ScheduleStoreRotation(tablet);
        };

        for (const auto& action : batch.Rotations) {
            const auto& lsmTablet = action.Tablet;

            if (slot) {
                const auto& tabletManager = slot->GetTabletManager();
                scheduleRotation(tabletManager, lsmTablet);
            } else {
                tabletsByCellId[lsmTablet->GetCellId()].push_back(lsmTablet);
            }
        }

        const auto& slotManager = Bootstrap_->GetSlotManager();
        for (const auto& [cellId, tablets] : tabletsByCellId) {
            auto slot = slotManager->FindSlot(cellId);
            if (!slot) {
                YT_LOG_DEBUG("Tablet cell is missing, aborting rotation (CellId: %v)",
                    cellId);
                continue;
            }

            auto invoker = slot->GetGuardedAutomatonInvoker();
            invoker->Invoke(BIND([
                slot = std::move(slot),
                tablets = std::move(tablets),
                scheduleRotation
            ] {
                if (slot->GetAutomatonState() != EPeerState::Leading) {
                    return;
                }

                const auto& tabletManager = slot->GetTabletManager();
                for (const auto& lsmTablet : tablets) {
                    scheduleRotation(tabletManager, lsmTablet);
                }
            }));
        }
    }
};

IStoreRotatorPtr CreateStoreRotator(IBootstrap* bootstrap)
{
    return New<TStoreRotator>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
