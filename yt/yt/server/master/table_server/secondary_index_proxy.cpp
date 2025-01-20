#include "private.h"
#include "secondary_index_proxy.h"
#include "secondary_index.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/table_server/replicated_table_node.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndexProxy
    : public TNonversionedObjectProxyBase<TSecondaryIndex>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TSecondaryIndex>;

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        const auto* secondaryIndex = GetThisImpl();

        attributes->push_back(EInternedAttributeKey::TableId);
        attributes->push_back(EInternedAttributeKey::TablePath);
        attributes->push_back(EInternedAttributeKey::IndexTableId);
        attributes->push_back(EInternedAttributeKey::IndexTablePath);
        attributes->push_back(EInternedAttributeKey::Kind);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TableToIndexCorrespondence)
            .SetWritable(true)
            .SetReplicated(true));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::UnfoldedColumn)
            .SetPresent(secondaryIndex->UnfoldedColumn().has_value()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Predicate)
            .SetPresent(secondaryIndex->Predicate().has_value()));

        TBase::ListSystemAttributes(attributes);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* secondaryIndex = GetThisImpl();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& tableManager = Bootstrap_->GetTableManager();

        switch (key) {
            case EInternedAttributeKey::TableId:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetTableId());
                return true;

            case EInternedAttributeKey::TablePath:
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(tableManager->GetTableNodeOrThrow(secondaryIndex->GetTableId()), /*transaction*/ nullptr));
                return true;

            case EInternedAttributeKey::IndexTableId:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetIndexTableId());
                return true;

            case EInternedAttributeKey::IndexTablePath:
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(tableManager->GetTableNodeOrThrow(secondaryIndex->GetIndexTableId()), /*transaction*/nullptr));
                return true;

            case EInternedAttributeKey::Kind:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetKind());
                return true;

            case EInternedAttributeKey::Predicate:
                if (secondaryIndex->Predicate()) {
                    BuildYsonFluently(consumer)
                        .Value(*secondaryIndex->Predicate());
                    return true;
                }
                return false;

            case EInternedAttributeKey::UnfoldedColumn:
                if (secondaryIndex->UnfoldedColumn()) {
                    BuildYsonFluently(consumer)
                        .Value(*secondaryIndex->UnfoldedColumn());
                    return true;
                }
                return false;

            case EInternedAttributeKey::TableToIndexCorrespondence:
                BuildYsonFluently(consumer)
                    .Value(secondaryIndex->GetTableToIndexCorrespondence());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* secondaryIndex = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::TableToIndexCorrespondence: {
                const auto& tableManager = Bootstrap_->GetTableManager();

                auto* table = tableManager->FindTableNode(secondaryIndex->GetTableId());

                YT_LOG_ALERT_IF(!IsObjectAlive(table), "Failed to find indexed table of secondary index (TableId: %v, IndexId: %v)",
                    secondaryIndex->GetTableId(),
                    secondaryIndex->GetId());

                THROW_ERROR_EXCEPTION_IF(!IsObjectAlive(table), "Failed to find table %v of secondary index %v",
                    secondaryIndex->GetTableId(),
                    secondaryIndex->GetId());

                table->ValidateAllTabletsUnmounted("Indexed table must be unmounted before index correspondence is changed");

                auto newCorrespondence = ConvertTo<ETableToIndexCorrespondence>(value);

                THROW_ERROR_EXCEPTION_IF(newCorrespondence == ETableToIndexCorrespondence::Unknown,
                    "Cannot set secondary index correspondence to table to %Qlv",
                    ETableToIndexCorrespondence::Unknown);

                if (newCorrespondence == ETableToIndexCorrespondence::Injective &&
                    secondaryIndex->GetKind() == ESecondaryIndexKind::Unique)
                {
                    THROW_ERROR_EXCEPTION("Secondary indices of %Qlv kind cannot have %Qlv correspondence",
                        ESecondaryIndexKind::Unique,
                        ETableToIndexCorrespondence::Injective);
                }

                secondaryIndex->SetTableToIndexCorrespondence(newCorrespondence);

                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateSecondaryIndexProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TSecondaryIndex* secondaryIndex)
{
    return New<TSecondaryIndexProxy>(bootstrap, metadata, secondaryIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
