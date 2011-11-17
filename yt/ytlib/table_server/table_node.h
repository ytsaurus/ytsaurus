#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/chunk_manager.h"
#include "../cypress/node_detail.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NCypress::TCypressNodeBase
{
    DECLARE_BYREF_RW_PROPERTY(ChunkListIds, yvector<NChunkServer::TChunkListId>);

public:
    explicit TTableNode(const NCypress::TBranchedNodeId& id);
    TTableNode(const NCypress::TBranchedNodeId& id, const TTableNode& other);

    virtual TAutoPtr<NCypress::ICypressNode> Clone() const;

    virtual NCypress::ERuntimeNodeType GetRuntimeType() const;

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

