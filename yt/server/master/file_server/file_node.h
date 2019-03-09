#pragma once

#include "public.h"

#include <yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/server/master/cypress_server/node_detail.h>

#include <yt/core/crypto/crypto.h>

namespace NYT::NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public NChunkServer::TChunkOwnerBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NCrypto::TMD5Hasher>, MD5Hasher);

    TFileNode* GetTrunkNode();
    const TFileNode* GetTrunkNode() const;

public:
    explicit TFileNode(const NCypressServer::TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual void EndUpload(const TEndUploadContext& context) override;
    virtual void GetUploadParams(std::optional<NCrypto::TMD5Hasher>* md5Hasher) override;
};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileServer

