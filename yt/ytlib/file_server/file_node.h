#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/chunk_manager.h"
#include "../cypress/node_detail.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NCypress::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkListId, ChunkListId);

public:
    explicit TFileNode(const NCypress::TBranchedNodeId& id);
    TFileNode(const NCypress::TBranchedNodeId& id, const TFileNode& other);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual NCypress::ERuntimeNodeType GetRuntimeType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateFileTypeHandler(
    NCypress::TCypressManager* cypressManager,
    NChunkServer::TChunkManager* chunkManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

