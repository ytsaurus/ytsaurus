#include "secondary_index_type_handler.h"
#include "secondary_index.h"
#include "secondary_index_proxy.h"
#include "table_manager.h"
#include "table_node.h"

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

class TSecondaryIndexTypeHandler
    : public TObjectTypeHandlerWithMapBase<TSecondaryIndex>
{
public:
    TSecondaryIndexTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TSecondaryIndex>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
        , Bootstrap_(bootstrap)
    { }

    EObjectType GetType() const override
    {
        return EObjectType::SecondaryIndex;
    }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto kind = attributes->GetAndRemove<ESecondaryIndexKind>(
            EInternedAttributeKey::Kind.Unintern(),
            ESecondaryIndexKind::FullSync);
        auto tableId = attributes->GetAndRemove<TTableId>(
            EInternedAttributeKey::TableId.Unintern());
        auto indexTableId = attributes->GetAndRemove<TTableId>(
            EInternedAttributeKey::IndexTableId.Unintern());

        const auto& tableManager = Bootstrap_->GetTableManager();

        auto* table = tableManager->GetTableNodeOrThrow(tableId);
        auto* indexTable = tableManager->GetTableNodeOrThrow(indexTableId);

        return tableManager->CreateSecondaryIndex(hintId, kind, table, indexTable);
    }

private:
    TBootstrap* const Bootstrap_;

    void DoZombifyObject(TSecondaryIndex* secondaryIndex) override
    {
        auto* table = secondaryIndex->GetTable();
        auto* index_table = secondaryIndex->GetIndexTable();
        if (table) {
            EraseOrCrash(table->MutableSecondaryIndices(), secondaryIndex);
            secondaryIndex->SetTable(nullptr);
        }
        if (index_table) {
            index_table->SetIndexTo(nullptr);
            secondaryIndex->SetIndexTable(nullptr);
        }

        TObjectTypeHandlerWithMapBase::DoZombifyObject(secondaryIndex);
    }

    IObjectProxyPtr DoGetProxy(TSecondaryIndex* secondaryIndex, TTransaction* /*transaction*/) override
    {
        return CreateSecondaryIndexProxy(Bootstrap_, &Metadata_, secondaryIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateSecondaryIndexTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TSecondaryIndex>* map)
{
    return New<TSecondaryIndexTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
