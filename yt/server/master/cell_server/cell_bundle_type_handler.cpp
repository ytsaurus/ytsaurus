#include "cell_bundle.h"
#include "cell_bundle_proxy.h"
#include "cell_bundle_type_handler.h"
#include "tamed_cell_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/tablet_client/config.h>

namespace NYT::NCellServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NYTree;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TCellBundleTypeHandlerBase<TImpl>::TCellBundleTypeHandlerBase(
    NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

template <class TImpl>
NObjectServer::ETypeFlags TCellBundleTypeHandlerBase<TImpl>::GetFlags() const
{
    return
        NObjectServer::ETypeFlags::ReplicateCreate |
            NObjectServer::ETypeFlags::ReplicateDestroy |
            NObjectServer::ETypeFlags::ReplicateAttributes |
            NObjectServer::ETypeFlags::Creatable |
            NObjectServer::ETypeFlags::Removable |
            NObjectServer::ETypeFlags::TwoPhaseRemoval;
}

template <class TImpl>
NObjectServer::TObject* TCellBundleTypeHandlerBase<TImpl>::DoCreateObject(
    std::unique_ptr<TCellBundle> holder,
    NYTree::IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");
    auto options = attributes->GetAndRemove<TTabletCellOptionsPtr>("options");

    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    return cellManager->CreateCellBundle(name, std::move(holder), std::move(options));
}

template <class TImpl>
NObjectServer::TObject* TCellBundleTypeHandlerBase<TImpl>::FindObject(NObjectClient::TObjectId id)
{
    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    return cellManager->FindCellBundle(id);
}

template <class TImpl>
NObjectClient::TCellTagList TCellBundleTypeHandlerBase<TImpl>::DoGetReplicationCellTags(const TImpl* /*cellBundle*/)
{
    return TBase::AllSecondaryCellTags();
}

template <class TImpl>
NSecurityServer::TAccessControlDescriptor* TCellBundleTypeHandlerBase<TImpl>::DoFindAcd(TImpl* cellBundle)
{
    return &cellBundle->Acd();
}

template <class TImpl>
void TCellBundleTypeHandlerBase<TImpl>::DoZombifyObject(TImpl* cellBundle)
{
    TBase::DoZombifyObject(cellBundle);
    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    cellManager->ZombifyCellBundle(cellBundle);
}

template <class TImpl>
void TCellBundleTypeHandlerBase<TImpl>::DoDestroyObject(TImpl* cellBundle)
{
    TBase::DoDestroyObject(cellBundle);
    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    cellManager->DestroyCellBundle(cellBundle);
}

////////////////////////////////////////////////////////////////////////////////

template class
TCellBundleTypeHandlerBase<NTabletServer::TTabletCellBundle>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
