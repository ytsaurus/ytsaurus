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

        descriptors->push_back("child_ids");
        descriptors->push_back("parent_ids");
        descriptors->push_back("statistics");
        descriptors->push_back(TAttributeDescriptor("tree")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("owning_nodes")
            .SetOpaque(true));
    }

    void TraverseTree(TChunkTree* chunkTree, NYson::IYsonConsumer* consumer)
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
                for (auto* child : chunkList->Children()) {
                    consumer->OnListItem();
                    TraverseTree(child, consumer);
                }
                consumer->OnEndList();
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto cypressManager = Bootstrap_->GetCypressManager();

        auto* chunkList = GetThisTypedImpl();

        if (key == "child_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Children(), [=] (TFluentList fluent, const TChunkTree* child) {
                    fluent.Item().Value(child->GetId());
            });
            return true;
        }

        if (key == "parent_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Parents(), [=] (TFluentList fluent, const TChunkList* chunkList) {
                    fluent.Item().Value(chunkList->GetId());
            });
            return true;
        }

        const auto& statistics = chunkList->Statistics();

        if (key == "statistics") {
            BuildYsonFluently(consumer)
                .Value(statistics);
            return true;
        }

        if (key == "tree") {
            TraverseTree(chunkList, consumer);
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override
    {
        auto* chunkList = GetThisTypedImpl();

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
