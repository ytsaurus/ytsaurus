#include "stdafx.h"
#include "store_compactor.h"
#include "config.h"
#include "tablet_cell_controller.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "private.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/fiber.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactor::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TStoreCompactorConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ThreadPool_(New<TThreadPool>(Config_->ThreadPoolSize, "StoreCompact"))
    { }

    void Start()
    {
        auto tabletCellController = Bootstrap_->GetTabletCellController();
        tabletCellController->SubscribeSlotScan(BIND(&TImpl::ScanSlot, MakeStrong(this)));
    }

private:
    TStoreCompactorConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    TThreadPoolPtr ThreadPool_;


    void ScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading)
            return;

        auto tabletManager = slot->GetTabletManager();
        auto tablets = tabletManager->Tablets().GetValues();
        for (auto* tablet : tablets) {
            ScanTablet(tablet);
        }
    }

    void ScanTablet(TTablet* tablet)
    {
        // TODO(babenko)
    }

};

////////////////////////////////////////////////////////////////////////////////

TStoreCompactor::TStoreCompactor(
    TStoreCompactorConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

TStoreCompactor::~TStoreCompactor()
{ }

void TStoreCompactor::Start()
{
    Impl_->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
