#include "cell_type_handler_base.h"
#include "cell_base.h"
#include "cell_proxy_base.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/chaos_server/chaos_cell.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/tablet_server/tablet_cell.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NCellServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TCellTypeHandlerBase<TImpl>::TCellTypeHandlerBase(
    NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

template <class TImpl>
NObjectServer::ETypeFlags TCellTypeHandlerBase<TImpl>::GetFlags() const
{
    return
        NObjectServer::ETypeFlags::ReplicateCreate |
            NObjectServer::ETypeFlags::ReplicateDestroy |
            NObjectServer::ETypeFlags::ReplicateAttributes |
            NObjectServer::ETypeFlags::Creatable |
            // XXX(babenko): two phase
            NObjectServer::ETypeFlags::Removable;
}

template <class TImpl>
NObjectServer::TObject* TCellTypeHandlerBase<TImpl>::DoCreateObject(
    std::unique_ptr<TCellBase> holder,
    NYTree::IAttributeDictionary* attributes)
{
    auto cellBundleName = attributes->FindAndRemove<TString>("cell_bundle");
    auto tabletCellBundleName = attributes->FindAndRemove<TString>("tablet_cell_bundle");
    if (cellBundleName && tabletCellBundleName) {
        THROW_ERROR_EXCEPTION("Only one of \"cell_bundle\" or \"tablet_cell_bundle\" should be specified");
    }

    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    auto* cellBundle = cellManager->GetCellBundleByNameOrThrow(
        cellBundleName.value_or(tabletCellBundleName.value_or(DefaultCellBundleName)),
        /*activeLifeStageOnly*/ true);

    auto areaName = attributes->GetAndRemove<TString>("area", DefaultAreaName);
    auto* area = cellBundle->GetAreaOrThrow(areaName);

    return cellManager->CreateCell(cellBundle, area, std::move(holder));
}

template <class TImpl>
NObjectServer::TObject* TCellTypeHandlerBase<TImpl>::FindObject(NObjectClient::TObjectId id)
{
    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    return cellManager->FindCell(id);
}

template <class TImpl>
NObjectClient::TCellTagList TCellTypeHandlerBase<TImpl>::DoGetReplicationCellTags(const TImpl* /*cell*/)
{
    return TBase::AllSecondaryCellTags();
}

template <class TImpl>
void TCellTypeHandlerBase<TImpl>::DoZombifyObject(TImpl* cell)
{
    TBase::DoZombifyObject(cell);
    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    cellManager->ZombifyCell(cell);
}

template <class TImpl>
void TCellTypeHandlerBase<TImpl>::DoDestroyObject(TImpl* cell)
{
    TBase::DoDestroyObject(cell);
    const auto& cellManager = TBase::Bootstrap_->GetTamedCellManager();
    cellManager->DestroyCell(cell);
}

////////////////////////////////////////////////////////////////////////////////

template class
TCellTypeHandlerBase<NTabletServer::TTabletCell>;

template class
TCellTypeHandlerBase<NChaosServer::TChaosCell>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
