#pragma once

#include "common.h"

#include <ytlib/misc/property.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/cypress/node_detail.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NCypress::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkListId, ChunkListId);

public:
    TTableNode(const NCypress::TVersionedNodeId& id, NCypress::EObjectType objectType);
    TTableNode(const NCypress::TVersionedNodeId& id, const TTableNode& other);

    virtual TAutoPtr<NCypress::ICypressNode> Clone() const;

    virtual NCypress::EObjectType GetObjectType() const;

    virtual void Save(TOutputStream* output) const;
    
    virtual void Load(TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateTableTypeHandler(
    NCypress::TCypressManager* cypressManager,
    NChunkServer::TChunkManager* chunkManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

