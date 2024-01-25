#include "cell_map_proxy.h"

#include "cell_base.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellarClient;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCellMapProxy
    : public TCypressMapNodeProxy
{
public:
    TCellMapProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TCypressMapNode* trunkNode,
        ECellarType cellarType)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
        , CellarType_(cellarType)
    { }

private:
    using TBase = TCypressMapNodeProxy;

    ECellarType CellarType_;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::CountByHealth);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        switch (key) {
            case EInternedAttributeKey::CountByHealth: {
                const auto& cellManager = Bootstrap_->GetTamedCellManager();
                TEnumIndexedArray<ECellHealth, int> counts;
                for (auto* cell : cellManager->Cells(CellarType_)) {
                    if (!IsObjectAlive(cell)) {
                        continue;
                    }
                    ++counts[cell->GetHealth()];
                }
                BuildYsonFluently(consumer)
                    .DoMapFor(TEnumTraits<ECellHealth>::GetDomainValues(), [&] (TFluentMap fluent, ECellHealth health) {
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

ICypressNodeProxyPtr CreateCellMapProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TCypressMapNode* trunkNode,
    ECellarType cellarType)
{
    return New<TCellMapProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode,
        cellarType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
