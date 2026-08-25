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

using NLsm::EStoreRotationReason;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

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

    void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) override
    {
        THashMap<TCellId, std::vector<NLsm::TRotateStoreRequest>> requestsByCellId;

        auto scheduleRotation = [] (const auto& tabletManager, const auto& request) {
            const auto& lsmTablet = request.Tablet;
            auto* tablet = tabletManager->FindTablet(lsmTablet->GetId());
            if (!tablet) {
                YT_TLOG_DEBUG("Tablet is missing, aborting rotation")
                    .With("TabletId", lsmTablet->GetId());
                return;
            }

            if (tablet->GetMountRevision() != lsmTablet->GetMountRevision()) {
                YT_TLOG_DEBUG("Mount revision mismatch, aborting rotation")
                    .With("ExpectedMountRevision", lsmTablet->GetMountRevision())
                    .With("ActualMountRevision", tablet->GetMountRevision());
                return;
            }

            auto activeStore = tablet->GetActiveStore();
            auto activeStoreId = activeStore ? activeStore->GetId() : TStoreId{};
            if (lsmTablet->FindActiveStore()->GetId() != activeStoreId) {
                YT_TLOG_DEBUG("Active store id mismatch, aborting rotation")
                    .With("ExpectedDynamicStoreId", lsmTablet->FindActiveStore()->GetId())
                    .With("ActualDynamicStoreId", activeStoreId);
            }

            if (request.Reason == EStoreRotationReason::Periodic) {
                const auto& storeManager = tablet->GetStoreManager();
                if (storeManager->GetLastPeriodicRotationTime() != request.ExpectedLastPeriodicRotationTime) {
                    YT_TLOG_DEBUG("Last periodic rotation time mismatch, aborting rotation")
                        .With(tablet->GetLoggingTags())
                        .With("ExpectedTime", request.ExpectedLastPeriodicRotationTime)
                        .With("ActualTime", storeManager->GetLastPeriodicRotationTime());
                    return;
                }

                storeManager->SetLastPeriodicRotationTime(request.NewLastPeriodicRotationTime);
            }

            tabletManager->ScheduleStoreRotation(tablet, request.Reason);
        };

        for (const auto& action : batch.Rotations) {
            const auto& lsmTablet = action.Tablet;

            if (slot) {
                const auto& tabletManager = slot->GetTabletManager();
                scheduleRotation(tabletManager, action);
            } else {
                requestsByCellId[lsmTablet->GetCellId()].push_back(action);
            }
        }

        const auto& slotManager = Bootstrap_->GetSlotManager();
        for (const auto& [cellId, requests] : requestsByCellId) {
            auto slot = slotManager->FindSlot(cellId);
            if (!slot) {
                YT_TLOG_DEBUG("Tablet cell is missing, aborting rotation")
                    .With("CellId", cellId);
                continue;
            }

            auto invoker = slot->GetGuardedAutomatonInvoker();
            invoker->Invoke(BIND([
                slot = std::move(slot),
                requests = std::move(requests),
                scheduleRotation
            ] {
                if (slot->GetAutomatonState() != EPeerState::Leading) {
                    return;
                }

                const auto& tabletManager = slot->GetTabletManager();
                for (const auto& request : requests) {
                    scheduleRotation(tabletManager, request);
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
