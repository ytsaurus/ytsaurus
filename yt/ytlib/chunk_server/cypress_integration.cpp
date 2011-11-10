#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"
#include "../ytree/virtual.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TChunkManager* chunkManager)
        : ChunkManager(chunkManager)
    { }

private:
    TChunkManager::TPtr ChunkManager;

    virtual yvector<Stroka> GetKeys()
    {
        auto ids = ChunkManager->GetChunkIds();
        yvector<Stroka> keys;
        keys.reserve(ids.ysize());
        FOREACH(const auto& id, ids) {
            keys.push_back(id.ToString());
        }
        return keys;
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TChunkId::FromString(key);
        auto* chunk = ChunkManager->FindChunk(id);
        if (chunk == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                // TODO: locations
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("size").Scalar(chunk->GetSize())
                        .Item("chunk_list_id").Scalar(chunk->GetChunkListId().ToString())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateChunkMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::ChunkMap,
        // TODO: extract type name
        "chunk_map",
        ~New<TVirtualChunkMap>(chunkManager));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkListMap(TChunkManager* chunkManager)
        : ChunkManager(chunkManager)
    { }

private:
    TChunkManager::TPtr ChunkManager;

    virtual yvector<Stroka> GetKeys()
    {
        auto ids = ChunkManager->GetChunkListIds();
        yvector<Stroka> keys;
        keys.reserve(ids.ysize());
        FOREACH(const auto& id, ids) {
            keys.push_back(id.ToString());
        }
        return keys;
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TChunkListId::FromString(key);
        auto* chunkList = ChunkManager->FindChunkList(id);
        if (chunkList == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                // TODO: ChunkIds
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("replica_count").Scalar(chunkList->GetReplicaCount())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::ChunkListMap,
        // TODO: extract type name
        "chunk_list_map",
        ~New<TVirtualChunkListMap>(chunkManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
