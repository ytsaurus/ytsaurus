#include "tablet_cell_map_proxy.h"
#include "tablet_manager.h"
#include "tablet_cell.h"

#include <yt/server/cypress_server/node_proxy_detail.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletServer {

using namespace NYson;
using namespace NYTree;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellMapProxy
    : public TMapNodeProxy
{
public:
    TTabletCellMapProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    typedef TMapNodeProxy TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::CountByHealth);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        switch (key) {
            case EInternedAttributeKey::CountByHealth: {
                const auto& tabletManager = Bootstrap_->GetTabletManager();
                TEnumIndexedVector<int, ETabletCellHealth> counts;
                for (const auto& pair : tabletManager->TabletCells()) {
                    const auto* cell = pair.second;
                    if (!IsObjectAlive(cell)) {
                        continue;
                    }
                    ++counts[cell->GetHealth()];
                }
                BuildYsonFluently(consumer)
                    .DoMapFor(TEnumTraits<ETabletCellHealth>::GetDomainValues(), [&] (TFluentMap fluent, ETabletCellHealth health) {
                        fluent
                            .Item(FormatEnum(health)).Value(counts[health]);
                    });
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateTabletCellMapProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TMapNode* trunkNode)
{
    return New<TTabletCellMapProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
