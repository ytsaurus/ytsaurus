#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <server/cypress_server/node_detail.h>

#include <server/cell_master/public.h>

#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NCypressServer::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(NFileClient::EFileUpdateMode, UpdateMode);
    DEFINE_BYVAL_RW_PROPERTY(int, ReplicationFactor);

public:
    explicit TFileNode(const NCypressServer::TVersionedNodeId& id);

    virtual int GetOwningReplicationFactor() const override;

    virtual NSecurityServer::TClusterResources GetResourceUsage() const override;

    virtual void Save(const NCellMaster::TSaveContext& context) const override;
    virtual void Load(const NCellMaster::TLoadContext& context) override;

private:
    const NChunkServer::TChunkList* GetUsageChunkList() const;

};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

