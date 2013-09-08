#include "stdafx.h"
#include "cypress_integration.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"

#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>

#include <server/cypress_server/virtual.h>
#include <server/cypress_server/node_proxy_detail.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TBootstrap* bootstrap, EObjectType type)
        : Bootstrap(bootstrap)
        , Type(type)
    { }

private:
    TBootstrap* Bootstrap;
    EObjectType Type;

    const yhash_set<TChunk*>& GetFilteredChunks() const
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        switch (Type) {
            case EObjectType::LostChunkMap:
                return chunkManager->LostChunks();
            case EObjectType::LostVitalChunkMap:
                return chunkManager->LostVitalChunks();
            case EObjectType::OverreplicatedChunkMap:
                return chunkManager->OverreplicatedChunks();
            case EObjectType::UnderreplicatedChunkMap:
                return chunkManager->UnderreplicatedChunks();
            case EObjectType::DataMissingChunkMap:
                return chunkManager->DataMissingChunks();
            case EObjectType::ParityMissingChunkMap:
                return chunkManager->ParityMissingChunks();
            default:
                YUNREACHABLE();
        }
    }

    bool CheckFilter(TChunk* chunk) const
    {
        if (Type == EObjectType::ChunkMap) {
            return true;
        }

        const auto& chunks = GetFilteredChunks();
        return chunks.find(chunk) != chunks.end();
    }

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        std::vector<TObjectId> ids;
        if (Type == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap->GetChunkManager();
            ids = ToObjectIds(chunkManager->GetChunks(sizeLimit));
        } else {
            const auto& chunks = GetFilteredChunks();
            // NB: |chunks| contains all the matching chunks, enforce size limit.
            ids = ToObjectIds(chunks, sizeLimit);
        }
        // NB: No size limit is needed here.
        return ConvertToStrings(ids);
    }

    virtual size_t GetSize() const override
    {
        if (Type == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap->GetChunkManager();
            return chunkManager->GetChunkCount();
        } else {
            return GetFilteredChunks().size();
        }
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TChunkId::FromString(key);

        auto chunkManager = Bootstrap->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(id);
        if (!IsObjectAlive(chunk)) {
            return nullptr;
        }

        if (!CheckFilter(chunk)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(chunk);
    }
};

INodeTypeHandlerPtr CreateChunkMapTypeHandler(TBootstrap* bootstrap, EObjectType type)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualChunkMap>(bootstrap, type);
    return CreateVirtualTypeHandler(
        bootstrap,
        type,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualChunkListMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto ids = ToObjectIds(chunkManager->GetChunkLists(sizeLimit));
        // NB: No size limit is needed here.
        return ConvertToStrings(ids);
    }

    virtual size_t GetSize() const override
    {
        return Bootstrap->GetChunkManager()->GetChunkListCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto id = TChunkListId::FromString(key);
        auto* chunkList = chunkManager->FindChunkList(id);
        if (!IsObjectAlive(chunkList)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(chunkList);
    }
};

INodeTypeHandlerPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualChunkListMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
