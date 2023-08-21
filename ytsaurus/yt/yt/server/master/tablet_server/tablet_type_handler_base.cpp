#include "tablet_type_handler_base.h"

#include "tablet_manager.h"
#include "hunk_tablet.h"

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TObject* TTabletTypeHandlerBase<TImpl>::FindObject(TObjectId id)
{
    const auto& tabletManager = TBase::Bootstrap_->GetTabletManager();
    return tabletManager->FindTablet(id);
}

template <class TImpl>
void TTabletTypeHandlerBase<TImpl>::DoDestroyObject(TImpl* tablet) noexcept
{
    const auto& tabletManager = TBase::Bootstrap_->GetTabletManager();
    tabletManager->DestroyTablet(tablet);

    TBase::DoDestroyObject(tablet);
}

template <class TImpl>
void TTabletTypeHandlerBase<TImpl>::CheckInvariants(TBootstrap* bootstrap)
{
    const auto& tabletManager = bootstrap->GetTabletManager();
    for (auto [tabletId, tablet] : tabletManager->Tablets()) {
        if (tablet->GetType() == this->GetType()) {
            tablet->CheckInvariants(bootstrap);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TTabletTypeHandlerBase<TTablet>;
template class TTabletTypeHandlerBase<THunkTablet>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
