#include "secondary_index_type_handler.h"
#include "secondary_index.h"
#include "secondary_index_proxy.h"
#include "table_manager.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/table_server/private.h>
#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TableServerLogger;

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
        ValidateUserAllowedToCreateSecondaryIndex();

        auto kind = attributes->GetAndRemove<ESecondaryIndexKind>(
            EInternedAttributeKey::Kind.Unintern(),
            ESecondaryIndexKind::FullSync);
        auto tableId = attributes->GetAndRemove<TTableId>(EInternedAttributeKey::TableId.Unintern());
        auto indexTableId = attributes->GetAndRemove<TTableId>(EInternedAttributeKey::IndexTableId.Unintern());
        auto predicate = attributes->FindAndRemove<TString>(EInternedAttributeKey::Predicate.Unintern());

        std::optional<TString> unfoldedColumn;
        if (kind == ESecondaryIndexKind::Unfolding) {
            unfoldedColumn = attributes->GetAndRemove<TString>(EInternedAttributeKey::UnfoldedColumn.Unintern());
        }

        return Bootstrap_->GetTableManager()->CreateSecondaryIndex(
            hintId,
            kind,
            tableId,
            indexTableId,
            std::move(predicate),
            std::move(unfoldedColumn));
    }

    void ValidateUserAllowedToCreateSecondaryIndex()
    {
        if (IsHiveMutation()) {
            return;
        }

        if (Bootstrap_->GetDynamicConfig()->AllowEveryoneCreateSecondaryIndices) {
            return;
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto* user = securityManager->GetAuthenticatedUser();

        THROW_ERROR_EXCEPTION_UNLESS(user->GetAllowCreateSecondaryIndices(),
            "Could not verify permission to create %Qlv for user %Qv. "
            "Refer to \"Secondary indices\" article in documentation",
            EObjectType::SecondaryIndex,
            user->GetName());
    }

private:
    TBootstrap* const Bootstrap_;

    void DoZombifyObject(TSecondaryIndex* secondaryIndex) override
    {
        const auto& tableManager = Bootstrap_->GetTableManager();
        auto* table = tableManager->FindTableNode(secondaryIndex->GetTableId());
        auto* indexTable = tableManager->FindTableNode(secondaryIndex->GetIndexTableId());
        if (table) {
            YT_LOG_DEBUG("Drop index links from table due to index removal (IndexId: %v, TableId: %v)",
                secondaryIndex->GetId(),
                secondaryIndex->GetTableId());
            EraseOrCrash(table->MutableSecondaryIndices(), secondaryIndex);
            secondaryIndex->SetTableId({});
        }
        if (indexTable) {
            YT_LOG_DEBUG("Drop index links from index table due to index removal (IndexId: %v, TableId: %v)",
                secondaryIndex->GetId(),
                secondaryIndex->GetIndexTableId());
            indexTable->SetIndexTo(nullptr);
            secondaryIndex->SetIndexTableId({});
        }

        TObjectTypeHandlerWithMapBase::DoZombifyObject(secondaryIndex);
    }

    IObjectProxyPtr DoGetProxy(TSecondaryIndex* secondaryIndex, TTransaction* /*transaction*/) override
    {
        return CreateSecondaryIndexProxy(Bootstrap_, &Metadata_, secondaryIndex);
    }

    TCellTagList DoGetReplicationCellTags(const TSecondaryIndex* secondaryIndex) override
    {
        auto cellTag = secondaryIndex->GetExternalCellTag();
        return cellTag == NObjectClient::NotReplicatedCellTagSentinel
            ? TCellTagList{}
            : TCellTagList{cellTag};
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
