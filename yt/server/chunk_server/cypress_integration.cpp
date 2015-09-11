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
using namespace NYPath;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualChunkMap(TBootstrap* bootstrap, INodePtr owningNode, EObjectType type)
        : TVirtualMulticellMapBase(bootstrap, owningNode)
        , Type_(type)
    { }

private:
    const EObjectType Type_;


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

    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        if (Type_ == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap_->GetChunkManager();
            return ToObjectIds(GetValues(chunkManager->Chunks(), sizeLimit));
        } else {
            const auto& chunks = GetFilteredChunks();
            // NB: |chunks| contains all the matching chunks, enforce size limit.
            return ToObjectIds(chunks, sizeLimit);
        }
    }

    virtual bool IsValid(TObjectBase* object) const
    {
        if (object->GetType() != EObjectType::Chunk) {
            return false;
        }

        if (Type_ == EObjectType::ChunkMap) {
            return true;
        }

        auto* chunk = static_cast<TChunk*>(object);
        const auto& chunks = GetFilteredChunks();
        return chunks.find(chunk) != chunks.end();
    }

    virtual i64 GetSize() const override
    {
        if (Type_ == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap_->GetChunkManager();
            return chunkManager->Chunks().GetSize();
        } else {
            return GetFilteredChunks().size();
        }
    }

    virtual NYPath::TYPath GetWellKnownPath() const override
    {
        switch (Type_) {
            case EObjectType::ChunkMap:
                return "//sys/chunks";
            case EObjectType::LostChunkMap:
                return "//sys/lost_chunks";
            case EObjectType::LostVitalChunkMap:
                return "//sys/lost_vital_chunks";
            case EObjectType::OverreplicatedChunkMap:
                return "//sys/overreplicated_chunks";
            case EObjectType::UnderreplicatedChunkMap:
                return "//sys/underreplicated_chunks";
            case EObjectType::DataMissingChunkMap:
                return "//sys/data_missing_chunks";
            case EObjectType::ParityMissingChunkMap:
                return "//sys/parity_missing_chunks";
            case EObjectType::QuorumMissingChunkMap:
                return "//sys/quorum_missing_chunks";
            case EObjectType::UnsafelyPlacedChunkMap:
                return "//sys/unsafely_placed_chunks";
            default:
                YUNREACHABLE();
        }
    }

};

INodeTypeHandlerPtr CreateChunkMapTypeHandler(
    TBootstrap* bootstrap,
    EObjectType type)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        type,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualChunkMap>(bootstrap, owningNode, type);
        }),
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualChunkListMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMulticellMapBase(bootstrap, owningNode)
    { }

private:
    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        return ToObjectIds(GetValues(chunkManager->ChunkLists(), sizeLimit));
    }

    virtual bool IsValid(TObjectBase* object) const
    {
        return object->GetType() == EObjectType::ChunkList;
    }

    virtual i64 GetSize() const override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->ChunkLists().GetSize();
    }

    virtual TYPath GetWellKnownPath() const override
    {
        return "//sys/chunk_lists";
    }

};

INodeTypeHandlerPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualChunkListMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
