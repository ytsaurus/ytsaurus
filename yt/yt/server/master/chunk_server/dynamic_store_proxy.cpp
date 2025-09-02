#include "dynamic_store_proxy.h"
#include "dynamic_store.h"
#include "chunk_list.h"
#include "helpers.h"
#include "chunk_owner_base.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/tablet_server/tablet.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NYTree;
using namespace NYson;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

class TDynamicStoreProxy
    : public TNonversionedObjectProxyBase<TDynamicStore>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TDynamicStore>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* dynamicStore = GetThisImpl();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OwningNodes)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletId));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkId)
            .SetPresent(dynamicStore->IsFlushed()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* dynamicStore = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::TabletId:
                BuildYsonFluently(consumer)
                    .Value(GetObjectId(dynamicStore->GetTablet()));
                return true;

            case EInternedAttributeKey::ChunkId:
                if (!dynamicStore->IsFlushed()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(GetObjectId(dynamicStore->FlushedChunk()));
                return true;

            case EInternedAttributeKey::ParentIds:
                BuildYsonFluently(consumer)
                    .DoListFor(dynamicStore->Parents(), [] (TFluentList fluent, const TChunkTree* parent) {
                        fluent
                            .Item().Value(parent->GetId());
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* dynamicStore = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::OwningNodes:
                return GetMulticellOwningNodes(Bootstrap_, dynamicStore);

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateDynamicStoreProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TDynamicStore* dynamicStore)
{
    return New<TDynamicStoreProxy>(bootstrap, metadata, dynamicStore);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
