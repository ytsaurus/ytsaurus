#include "chunk_view_proxy.h"
#include "chunk_list.h"
#include "chunk_view.h"
#include "helpers.h"

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TChunkViewProxy
    : public TNonversionedObjectProxyBase<TChunkView>
{
public:
    TChunkViewProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TChunkView* chunkView)
        : TBase(bootstrap, metadata, chunkView)
    { }

private:
    using TBase = TNonversionedObjectProxyBase<TChunkView>;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkId));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OwningNodes)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LowerLimit));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UpperLimit));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* chunkView = GetThisImpl();
        const auto& readRange = chunkView->ReadRange();

        switch (key) {
            case EInternedAttributeKey::ChunkId: {
                auto underlyingChunk = chunkView->GetUnderlyingChunk();
                YCHECK(underlyingChunk);
                BuildYsonFluently(consumer)
                    .Value(underlyingChunk->GetId());
                return true;
            }

            case EInternedAttributeKey::ParentIds:
                BuildYsonFluently(consumer)
                    .DoListFor(chunkView->Parents(), [] (TFluentList fluent, const TChunkList* parent) {
                        fluent
                            .Item().Value(parent->GetId());
                    });
                return true;

            case EInternedAttributeKey::LowerLimit:
                BuildYsonFluently(consumer)
                    .Value(readRange.LowerLimit());
                return true;

            case EInternedAttributeKey::UpperLimit:
                BuildYsonFluently(consumer)
                    .Value(readRange.UpperLimit());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* chunkView = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::OwningNodes:
                return GetMulticellOwningNodes(Bootstrap_, chunkView);

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }
};

IObjectProxyPtr CreateChunkViewProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TChunkView* chunkView)
{
    return New<TChunkViewProxy>(bootstrap, metadata, chunkView);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
