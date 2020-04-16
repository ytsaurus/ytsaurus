#include "chunk_list_proxy.h"
#include "private.h"
#include "chunk_view.h"
#include "dynamic_store.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "helpers.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/tablet_server/tablet.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_detail.h>

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
    using TBase = TNonversionedObjectProxyBase<TChunkList>;

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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CumulativeStatistics)
            .SetPresent(chunkList->HasCumulativeStatistics())
            .SetOpaque(true));
    }

    void TraverseTree(const TChunkTree* chunkTree, NYson::IYsonConsumer* consumer)
    {
        switch (chunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                BuildYsonFluently(consumer)
                    .Value(chunkTree->GetId());
                break;
            }

            case EObjectType::ChunkView: {
                const auto* chunkView = chunkTree->AsChunkView();
                const auto& readRange = chunkView->ReadRange();
                BuildYsonFluently(consumer)
                    .BeginAttributes()
                        .Item("id").Value(chunkView->GetId())
                        .Item("type").Value("chunk_view")
                        .DoIf(readRange.LowerLimit().HasKey(), [&] (TFluentMap fluent) {
                            fluent.Item("lower_limit").Value(readRange.LowerLimit().GetKey());
                        })
                        .DoIf(readRange.UpperLimit().HasKey(), [&] (TFluentMap fluent) {
                            fluent.Item("upper_limit").Value(readRange.UpperLimit().GetKey());
                        })
                    .EndAttributes()
                    .BeginList()
                        .Item().Do([&] (TFluentAny fluent) {
                            TraverseTree(chunkView->GetUnderlyingChunk(), fluent.GetConsumer());
                        })
                    .EndList();
                break;
            }

            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore: {
                const auto* dynamicStore = chunkTree->AsDynamicStore();
                BuildYsonFluently(consumer)
                    .BeginAttributes()
                        .Item("type").Value("dynamic_store")
                        .Item("flushed").Value(dynamicStore->IsFlushed())
                        .Item("tablet_id").Value(GetObjectId(dynamicStore->GetTablet()))
                        .DoIf(dynamicStore->IsFlushed(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("chunk_id").Value(GetObjectId(dynamicStore->GetFlushedChunk()));
                        })
                    .EndAttributes()
                    .Value(dynamicStore->GetId());
                break;
            }

            case EObjectType::ChunkList: {
                const auto* chunkList = chunkTree->AsChunkList();
                BuildYsonFluently(consumer)
                    .BeginAttributes()
                        .Item("id").Value(chunkList->GetId())
                        .Item("rank").Value(chunkList->Statistics().Rank)
                        .Item("type").Value("chunk_list")
                    .EndAttributes()
                    .DoListFor(chunkList->Children(), [&] (TFluentList fluent, const TChunkTree* child) {
                        if (child) {
                            TraverseTree(child, fluent.GetConsumer());
                        } else {
                            fluent.Item().Entity();
                        }
                    });
                break;
            }

            default:
                YT_ABORT();
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
            case EInternedAttributeKey::CumulativeStatistics: {
                if (!chunkList->HasCumulativeStatistics()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunkList->CumulativeStatistics());
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
