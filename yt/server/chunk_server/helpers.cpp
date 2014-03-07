#include "stdafx.h"
#include "helpers.h"
#include "chunk_owner_base.h"

#include <core/ytree/fluent.h>

#include <ytlib/object_client/public.h>

#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            child->AsChunk()->Parents().push_back(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->Parents().insert(parent);
            break;
        default:
            YUNREACHABLE();
    }
}

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return chunkTree->AsChunk()->GetStatistics();
        case EObjectType::ChunkList:
            return chunkTree->AsChunkList()->Statistics();
        default:
            YUNREACHABLE();
    }
}

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children,
    bool resetSorted)
{
    AttachToChunkList(
        chunkList,
        const_cast<TChunkTree**>(children.data()),
        const_cast<TChunkTree**>(children.data() + children.size()),
        [] (TChunkTree* /*chunk*/) { },
        resetSorted);
}

void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* child,
    bool resetSorted)
{
    AttachToChunkList(
        chunkList,
        &child,
        &child + 1,
        [] (TChunkTree* /*chunk*/) { },
        resetSorted);
}

void GetOwningNodes(
    TChunkTree* chunkTree,
    yhash_set<TChunkTree*>& visited,
    yhash_set<TChunkOwnerBase*>* owningNodes)
{
    if (!visited.insert(chunkTree).second) {
        return;
    }
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk: {
            for (auto* parent : chunkTree->AsChunk()->Parents()) {
                GetOwningNodes(parent, visited, owningNodes);
            }
            break;
        }
        case EObjectType::ChunkList: {
            auto* chunkList = chunkTree->AsChunkList();
            owningNodes->insert(chunkList->OwningNodes().begin(), chunkList->OwningNodes().end());
            for (auto* parent : chunkList->Parents()) {
                GetOwningNodes(parent, visited, owningNodes);
            }
            break;
        }
        default:
            YUNREACHABLE();
    }
}

void SerializeOwningNodesPaths(
    TCypressManagerPtr cypressManager,
    TChunkTree* chunkTree,
    IYsonConsumer* consumer)
{
    yhash_set<TChunkOwnerBase*> owningNodes;
    yhash_set<TChunkTree*> visited;
    GetOwningNodes(chunkTree, visited, &owningNodes);

    BuildYsonFluently(consumer)
        .DoListFor(owningNodes, [&] (TFluentList fluent, TChunkOwnerBase* node) {
            auto proxy = cypressManager->GetNodeProxy(
                node->GetTrunkNode(),
                node->GetTransaction());
            auto path = proxy->GetPath();
            if (node->GetTransaction()) {
                fluent
                    .Item()
                    .BeginAttributes()
                        .Item("transaction_id").Value(node->GetTransaction()->GetId())
                    .EndAttributes()
                    .Value(path);
            } else {
                fluent
                    .Item()
                    .Value(path);
            }
        });
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
