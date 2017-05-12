#include "chunk_list_proxy.h"
#include "private.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "helpers.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NChunkServer {

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

        descriptors->push_back("child_ids");
        descriptors->push_back("child_count");
        descriptors->push_back("trimmed_child_count");
        descriptors->push_back("parent_ids");
        descriptors->push_back("statistics");
        descriptors->push_back("kind");
        descriptors->push_back(TAttributeDescriptor("pivot_key")
            .SetPresent(chunkList->GetKind() == EChunkListKind::SortedDynamicTablet));
        descriptors->push_back(TAttributeDescriptor("tree")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("owning_nodes")
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

    virtual bool GetBuiltinAttribute(const TString& key, NYson::IYsonConsumer* consumer) override
    {
        auto* chunkList = GetThisImpl();

        if (key == "child_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Children(), [=] (TFluentList fluent, const TChunkTree* child) {
                    if (child) {
                        fluent
                            .Item().Value(child->GetId());
                    }
                });
            return true;
        }

        if (key == "child_count") {
            BuildYsonFluently(consumer)
                .Value(chunkList->Children().size());
            return true;
        }

        if (key == "trimmed_child_count") {
            BuildYsonFluently(consumer)
                .Value(chunkList->GetTrimmedChildCount());
            return true;
        }

        if (key == "parent_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Parents(), [=] (TFluentList fluent, const TChunkList* chunkList) {
                    fluent.Item().Value(chunkList->GetId());
                });
            return true;
        }

        if (key == "statistics") {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            Serialize(chunkList->Statistics(), consumer, chunkManager);
            return true;
        }

        if (key == "kind") {
            BuildYsonFluently(consumer)
                .Value(chunkList->GetKind());
            return true;
        }

        if (key == "pivot_key" && chunkList->GetKind() == EChunkListKind::SortedDynamicTablet) {
            BuildYsonFluently(consumer)
                .Value(chunkList->GetPivotKey());
            return true;
        }

        if (key == "tree") {
            TraverseTree(chunkList, consumer);
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const TString& key) override
    {
        auto* chunkList = GetThisImpl();

        if (key == "owning_nodes") {
            return GetMulticellOwningNodes(Bootstrap_, chunkList);
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

} // namespace NChunkServer
} // namespace NYT
