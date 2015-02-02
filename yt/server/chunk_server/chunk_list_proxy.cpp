#include "stdafx.h"
#include "chunk_list_proxy.h"
#include "private.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "helpers.h"

#include <core/ytree/fluent.h>

#include <ytlib/chunk_client/chunk_list_ypath.pb.h>

#include <server/cell_master/bootstrap.h>

#include <server/object_server/object_detail.h>

#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TChunkListProxy
    : public TNonversionedObjectProxyBase<TChunkList>
{
public:
    TChunkListProxy(NCellMaster::TBootstrap* bootstrap, TChunkList* chunkList)
        : TBase(bootstrap, chunkList)
    { }

private:
    typedef TNonversionedObjectProxyBase<TChunkList> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return ChunkServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("children_ids");
        attributes->push_back("parent_ids");
        attributes->push_back("statistics");
        attributes->push_back(TAttributeInfo("tree", true, true));
        attributes->push_back(TAttributeInfo("owning_nodes", true, true));
        TBase::ListSystemAttributes(attributes);
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

        const auto* chunkList = GetThisTypedImpl();

        if (key == "children_ids") {
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
            TraverseTree(const_cast<TChunkList*>(chunkList), consumer);
            return true;
        }

        if (key == "owning_nodes") {
            SerializeOwningNodesPaths(
                cypressManager,
                const_cast<TChunkList*>(chunkList),
                consumer);
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Attach);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Attach)
    {
        UNUSED(response);

        DeclareMutating();

        auto childrenIds = FromProto<TChunkTreeId>(request->children_ids());

        context->SetRequestInfo("Children: [%v]", JoinToString(childrenIds));

        auto objectManager = Bootstrap_->GetObjectManager();
        auto chunkManager = Bootstrap_->GetChunkManager();

        std::vector<TChunkTree*> children;
        children.reserve(childrenIds.size());
        for (const auto& childId : childrenIds) {
            auto* child = chunkManager->GetChunkTreeOrThrow(childId);
            children.push_back(child);
        }

        auto* chunkList = GetThisTypedImpl();
        chunkManager->AttachToChunkList(chunkList, children);

        context->Reply();
    }

};

IObjectProxyPtr CreateChunkListProxy(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList)
{
    return New<TChunkListProxy>(bootstrap, chunkList);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
