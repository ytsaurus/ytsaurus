#include "chunk_list_proxy.h"
#include "private.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "helpers.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TChunkListProxy
    : public TNonversionedObjectProxyBase<TChunkList>
{
public:
    TChunkListProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TChunkList* chunkList)
        : TBase(bootstrap, metadata, chunkList)
    { }

private:
    typedef TNonversionedObjectProxyBase<TChunkList> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* chunkList = GetThisImpl();

        descriptors->push_back(EInternedAttributeKey::ChildIds);
        descriptors->push_back(EInternedAttributeKey::ChildCount);
        descriptors->push_back(EInternedAttributeKey::TrimmedChildCount);
        descriptors->push_back(EInternedAttributeKey::ParentIds);
        descriptors->push_back(EInternedAttributeKey::Statistics);
        descriptors->push_back(EInternedAttributeKey::Kind);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PivotKey)
            .SetPresent(chunkList->GetKind() == EChunkListKind::SortedDynamicTablet));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Tree)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OwningNodes)
            .SetOpaque(true));
    }

    void TraverseTree(const TChunkTree* chunkTree, NYson::IYsonConsumer* consumer)
    {
        switch (chunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                consumer->OnStringScalar(ToString(chunkTree->GetId()));
                break;
            }

            case EObjectType::ChunkList: {
                const auto* chunkList = chunkTree->AsChunkList();
                consumer->OnBeginAttributes();
                consumer->OnKeyedItem("id");
                consumer->OnStringScalar(ToString(chunkList->GetId()));
                consumer->OnKeyedItem("rank");
                consumer->OnInt64Scalar(chunkList->Statistics().Rank);
                consumer->OnEndAttributes();

                consumer->OnBeginList();
                for (const auto* child : chunkList->Children()) {
                    consumer->OnListItem();
                    if (child) {
                        TraverseTree(child, consumer);
                    } else {
                        consumer->OnEntity();
                    }
                }
                consumer->OnEndList();
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        auto* chunkList = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ChildIds:
                BuildYsonFluently(consumer)
                    .DoListFor(chunkList->Children(), [=] (TFluentList fluent, const TChunkTree* child) {
                        if (child) {
                            fluent
                                .Item().Value(child->GetId());
                        }
                    });
                return true;

            case EInternedAttributeKey::ChildCount:
                BuildYsonFluently(consumer)
                    .Value(chunkList->Children().size());
                return true;

            case EInternedAttributeKey::TrimmedChildCount:
                BuildYsonFluently(consumer)
                    .Value(chunkList->GetTrimmedChildCount());
                return true;

            case EInternedAttributeKey::ParentIds:
                BuildYsonFluently(consumer)
                    .DoListFor(chunkList->Parents(), [=] (TFluentList fluent, const TChunkList* chunkList) {
                        fluent.Item().Value(chunkList->GetId());
                    });
                return true;

            case EInternedAttributeKey::Statistics: {
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                Serialize(chunkList->Statistics(), consumer, chunkManager);
                return true;
            }

            case EInternedAttributeKey::Kind:
                BuildYsonFluently(consumer)
                    .Value(chunkList->GetKind());
                return true;

            case EInternedAttributeKey::PivotKey:
                if (chunkList->GetKind() != EChunkListKind::SortedDynamicTablet) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunkList->GetPivotKey());
                return true;

            case EInternedAttributeKey::Tree:
                TraverseTree(chunkList, consumer);
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* chunkList = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::OwningNodes:
                return GetMulticellOwningNodes(Bootstrap_, chunkList);

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }
};

IObjectProxyPtr CreateChunkListProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TChunkList* chunkList)
{
    return New<TChunkListProxy>(bootstrap, metadata, chunkList);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
