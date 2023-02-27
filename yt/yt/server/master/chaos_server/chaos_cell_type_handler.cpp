#include "chaos_cell_type_handler.h"

#include "chaos_cell.h"
#include "chaos_cell_proxy.h"
#include "chaos_manager.h"

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/yt/server/master/cell_server/cell_type_handler_base.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NChaosServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellTypeHandler
    : public TCellTypeHandlerBase<TChaosCell>
{
public:
    using TCellTypeHandlerBase::TCellTypeHandlerBase;

    EObjectType GetType() const override
    {
        return EObjectType::ChaosCell;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        if (!attributes->Contains("id") && !hintId) {
            THROW_ERROR_EXCEPTION("Must provide a valid chaos cell id");
        }

        auto id = attributes->GetAndRemove("id", hintId);
        if (TypeFromId(id) != EObjectType::ChaosCell || IsWellKnownId(id)) {
            THROW_ERROR_EXCEPTION("Malformed chaos cell id %v",
                id);
        }

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        if (cellManager->FindCell(id)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Cell with id %v already exists",
                id);
        }

        auto cellTag = CellTagFromId(id);
        if (cellManager->FindCellByCellTag(cellTag)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Cell with tag %v already exists",
                cellTag);
        }

        if (hintId && hintId != id) {
            THROW_ERROR_EXCEPTION("Wrong chaos cell hint id: expected %v, actual %v",
                hintId,
                id);
        }

        return DoCreateObject(id, attributes);
    }

private:
    using TBase = TCellTypeHandlerBase<TChaosCell>;

    TString DoGetName(const TChaosCell* cell) override
    {
        return Format("chaos cell %v", cell->GetId());
    }

    TString DoGetPath(const TChaosCell* cell) override
    {
        return Format("//sys/chaos_cells/%v", cell->GetId());
    }

    IObjectProxyPtr DoGetProxy(TChaosCell* cell, TTransaction* /*transaction*/) override
    {
        return CreateChaosCellProxy(Bootstrap_, &Metadata_, cell);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateChaosCellTypeHandler(
    TBootstrap* bootstrap)
{
    return New<TChaosCellTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
