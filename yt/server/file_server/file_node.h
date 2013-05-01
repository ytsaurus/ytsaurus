#pragma once

#include "public.h"

#include <server/chunk_server/chunk_owner_base.h>

#include <server/cypress_server/node_detail.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NChunkServer::TChunkOwnerBase
{
public:
    explicit TFileNode(const NCypressServer::TVersionedNodeId& id);

};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

