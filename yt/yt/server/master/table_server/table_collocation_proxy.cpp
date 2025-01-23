#include "table_collocation_proxy.h"
#include "table_collocation.h"
#include "table_manager.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/table_server/replicated_table_node.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TTableCollocationProxy
    : public TNonversionedObjectProxyBase<TTableCollocation>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TTableCollocation>;

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);

        const auto* collocation = GetThisImpl();
        for (const auto* table : GetValuesSortedByKey(collocation->Tables())) {
            if (table->GetIndexTo() || !table->SecondaryIndices().empty()) {
                THROW_ERROR_EXCEPTION("Cannot remove collocation %v because table %v has an index or is one itself",
                    collocation->GetId(),
                    table->GetId());
            }
        }
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        const auto* collocation = GetThisImpl();

        attributes->push_back(EInternedAttributeKey::ExternalCellTag);
        attributes->push_back(EInternedAttributeKey::TableIds);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TablePaths)
            .SetOpaque(true));
        attributes->push_back(EInternedAttributeKey::CollocationType);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicatedTableOptions)
            .SetWritable(true)
            .SetPresent(collocation->GetType() == ETableCollocationType::Replication));

        TBase::ListSystemAttributes(attributes);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* collocation = GetThisImpl();

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        switch (key) {
            case EInternedAttributeKey::ExternalCellTag:
                BuildYsonFluently(consumer)
                    .Value(collocation->GetExternalCellTag());
                return true;

            case EInternedAttributeKey::TableIds:
                BuildYsonFluently(consumer)
                    .DoListFor(collocation->Tables(), [] (TFluentList fluent, TTableNode* table) {
                        if (!IsObjectAlive(table)) {
                            return;
                        }
                        fluent
                            .Item().Value(table->GetId());
                    });
                return true;

            case EInternedAttributeKey::TablePaths:
                BuildYsonFluently(consumer)
                    .DoListFor(collocation->Tables(), [&] (TFluentList fluent, TTableNode* table) {
                        if (!IsObjectAlive(table)) {
                            return;
                        }
                        fluent
                            .Item().Value(cypressManager->GetNodePath(table, nullptr));
                    });
                return true;

            case EInternedAttributeKey::CollocationType:
                BuildYsonFluently(consumer)
                    .Value(collocation->GetType());
                return true;

            case EInternedAttributeKey::ReplicatedTableOptions:
                if (collocation->GetType() != ETableCollocationType::Replication) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(collocation->ReplicationCollocationOptions());

                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* collocation = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ReplicatedTableOptions: {
                ValidateNoTransaction();

                if (collocation->GetType() != ETableCollocationType::Replication) {
                    break;
                }

                auto options = ConvertTo<NTabletClient::TReplicationCollocationOptionsPtr>(value);

                const auto& tableManager = Bootstrap_->GetTableManager();
                tableManager->UpdateReplicationCollocationOptions(collocation, std::move(options));

                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateTableCollocationProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTableCollocation* collocation)
{
    return New<TTableCollocationProxy>(bootstrap, metadata, collocation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
