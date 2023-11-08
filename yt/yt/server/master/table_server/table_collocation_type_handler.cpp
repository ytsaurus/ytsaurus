#include "table_collocation_type_handler.h"
#include "table_collocation.h"
#include "table_collocation_proxy.h"
#include "table_manager.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TTableCollocationTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTableCollocation>
{
public:
    TTableCollocationTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TTableCollocation>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
        , Bootstrap_(bootstrap)
    { }

    EObjectType GetType() const override
    {
        return EObjectType::TableCollocation;
    }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto type = attributes->GetAndRemove<ETableCollocationType>(
            EInternedAttributeKey::CollocationType.Unintern());
        auto tableIds = attributes->GetAndRemove<std::vector<TTableId>>(
            EInternedAttributeKey::TableIds.Unintern());

        const auto& tableManager = Bootstrap_->GetTableManager();

        THashSet<TTableNode*> collocatedTables;
        collocatedTables.reserve(tableIds.size());
        for (auto tableId : tableIds) {
            auto* table = tableManager->GetTableNodeOrThrow(tableId);
            if (!collocatedTables.insert(table).second) {
                THROW_ERROR_EXCEPTION("Duplicate table %v",
                    tableId);
            }
        }

        return tableManager->CreateTableCollocation(hintId, type, std::move(collocatedTables));
    }

private:
    TBootstrap* const Bootstrap_;


    IObjectProxyPtr DoGetProxy(TTableCollocation* collocation, TTransaction* /*transaction*/) override
    {
        return CreateTableCollocationProxy(Bootstrap_, &Metadata_, collocation);
    }

    void DoZombifyObject(TTableCollocation* collocation) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(collocation);
        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ZombifyTableCollocation(collocation);
    }

    TCellTagList DoGetReplicationCellTags(const TTableCollocation* collocation) override
    {
        if (Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicateTableCollocations) {
            // NB: Cell tag may be invalid here only if creation has failed.
            auto cellTag = collocation->GetExternalCellTag();
            return cellTag == InvalidCellTag
                ? TCellTagList{}
                : TCellTagList{cellTag};
        } else {
            return EmptyCellTags();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateTableCollocationTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TTableCollocation>* map)
{
    return New<TTableCollocationTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
