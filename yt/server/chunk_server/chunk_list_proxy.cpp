#include "stdafx.h"
#include "chunk_list_proxy.h"
#include "private.h"
#include "chunk_list.h"
#include "chunk_manager.h"

#include <ytlib/chunk_client/chunk_list_ypath.pb.h>

#include <server/cell_master/bootstrap.h>

#include <server/object_server/object_detail.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TChunkListProxy
    : public TUnversionedObjectProxyBase<TChunkList>
{
public:
    TChunkListProxy(
        NCellMaster::TBootstrap* bootstrap,
        TMap* map,
        const TChunkListId& id)
        : TBase(bootstrap, id, map)
    {
        Logger = ChunkServerLogger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Attach);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TChunkList> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override
    {
        attributes->push_back("children_ids");
        attributes->push_back("parent_ids");
        attributes->push_back("statistics");
        attributes->push_back("rigid");
        attributes->push_back(TAttributeInfo("tree", true, true));
        attributes->push_back(TAttributeInfo("owning_nodes", true, true));
        TBase::ListSystemAttributes(attributes);
    }

    void TraverseTree(TChunkTreeRef ref, NYTree::IYsonConsumer* consumer) const
    {
        switch (ref.GetType()) {
            case EObjectType::Chunk: {
                consumer->OnStringScalar(ref.GetId().ToString());
                break;
            }

            case EObjectType::ChunkList: {
                const auto* chunkList = ref.AsChunkList();
                consumer->OnBeginAttributes();
                consumer->OnKeyedItem("id");
                consumer->OnStringScalar(chunkList->GetId().ToString());
                consumer->OnKeyedItem("rank");
                consumer->OnIntegerScalar(chunkList->Statistics().Rank);
                consumer->OnEndAttributes();

                consumer->OnBeginList();
                FOREACH (auto childRef, chunkList->Children()) {
                    consumer->OnListItem();
                    TraverseTree(childRef, consumer);
                }
                consumer->OnEndList();
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    virtual bool GetSystemAttribute(const Stroka& key, NYTree::IYsonConsumer* consumer) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        const auto* chunkList = GetTypedImpl();

        if (key == "children_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Children(), [=] (TFluentList fluent, TChunkTreeRef chunkRef) {
                    fluent.Item().Scalar(chunkRef.GetId());
            });
            return true;
        }

        if (key == "parent_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Parents(), [=] (TFluentList fluent, TChunkList* chunkList) {
                    fluent.Item().Scalar(chunkList->GetId());
            });
            return true;
        }

        const auto& statistics = chunkList->Statistics();

        if (key == "statistics") {
            BuildYsonFluently(consumer)
                .Scalar(statistics);
            return true;
        }

        if (key == "rigid") {
            BuildYsonFluently(consumer)
                .Scalar(chunkList->GetRigid());
            return true;
        }

        if (key == "tree") {
            TraverseTree(const_cast<TChunkList*>(chunkList), consumer);
            return true;
        }

        if (key == "owning_nodes") {
            chunkManager->GetOwningNodes(const_cast<TChunkList*>(chunkList), consumer);
            return true;
        }

        return TBase::GetSystemAttribute(key, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Attach);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Attach)
    {
        UNUSED(response);

        auto childrenIds = FromProto<TChunkTreeId>(request->children_ids());

        context->SetRequestInfo("Children: [%s]", ~JoinToString(childrenIds));

        auto objectManager = Bootstrap->GetObjectManager();
        auto chunkManager = Bootstrap->GetChunkManager();

        std::vector<TChunkTreeRef> childrenRefs;
        FOREACH (const auto& childId, childrenIds) {
            if (!objectManager->ObjectExists(childId)) {
                THROW_ERROR_EXCEPTION("Chunk tree %s does not exist", ~childId.ToString());
            }
            auto chunkRef = chunkManager->GetChunkTree(childId);
            childrenRefs.push_back(chunkRef);
        }

        auto* chunkList = GetTypedImpl();
        chunkManager->AttachToChunkList(chunkList, childrenRefs);

        context->Reply();
    }

};

IObjectProxyPtr CreateChunkListProxy(
    NCellMaster::TBootstrap* bootstrap,
    NMetaState::TMetaStateMap<TChunkListId, TChunkList>* map,
    const TChunkListId& id)
{
    return New<TChunkListProxy>(
        bootstrap,
        map,
        id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
