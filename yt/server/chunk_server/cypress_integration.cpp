#include "stdafx.h"
#include "cypress_integration.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"

#include <core/misc/collection_helpers.h>

#include <server/cypress_server/virtual.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TBootstrap* bootstrap, EObjectType type)
        : Bootstrap_(bootstrap)
        , Type_(type)
    { }

private:
    TBootstrap* Bootstrap_;
    EObjectType Type_;


    const yhash_set<TChunk*>& GetFilteredChunks() const
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        switch (Type_) {
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
            case EObjectType::QuorumMissingChunkMap:
                return chunkManager->QuorumMissingChunks();
            case EObjectType::UnsafelyPlacedChunkMap:
                return chunkManager->UnsafelyPlacedChunks();
            default:
                YUNREACHABLE();
        }
    }

    bool CheckFilter(TChunk* chunk) const
    {
        if (Type_ == EObjectType::ChunkMap) {
            return true;
        }

        const auto& chunks = GetFilteredChunks();
        return chunks.find(chunk) != chunks.end();
    }

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        std::vector<TObjectId> ids;
        if (Type_ == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap_->GetChunkManager();
            ids = ToObjectIds(GetValues(chunkManager->Chunks(), sizeLimit));
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
        if (Type_ == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap_->GetChunkManager();
            return chunkManager->Chunks().GetSize();
        } else {
            return GetFilteredChunks().size();
        }
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TChunkId::FromString(key);

        auto chunkManager = Bootstrap_->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(id);
        if (!IsObjectAlive(chunk)) {
            return nullptr;
        }

        if (!CheckFilter(chunk)) {
            return nullptr;
        }

        auto objectManager = Bootstrap_->GetObjectManager();
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

INodeTypeHandlerPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = CreateVirtualObjectMap(
        bootstrap,
        bootstrap->GetChunkManager()->ChunkLists());
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
