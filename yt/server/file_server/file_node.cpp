#include "file_node.h"
#include "private.h"
#include "file_node_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public TChunkOwnerTypeHandler<TFileNode>
{
public:
    explicit TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TChunkOwnerTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::File;
    }

    virtual bool IsExternalizable() const override
    {
        return true;
    }

protected:
    using TBase = TChunkOwnerTypeHandler<TFileNode>;

    virtual ICypressNodeProxyPtr DoGetProxy(
        TFileNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateFileNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TFileNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        TTransaction* transaction,
        IAttributeDictionary* attributes,
        TAccount* account,
        bool enableAccounting) override
    {
        const auto& config = this->Bootstrap_->GetConfig()->CypressManager;

        auto replicationFactor = attributes->GetAndRemove("replication_factor", config->DefaultFileReplicationFactor);
        auto compressionCodec = attributes->GetAndRemove<NCompression::ECodec>("compression_codec", NCompression::ECodec::None);
        auto erasureCodec = attributes->GetAndRemove<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

        ValidateReplicationFactor(replicationFactor);

        return DoCreateImpl(
            id,
            cellTag,
            transaction,
            attributes,
            account,
            enableAccounting,
            replicationFactor,
            compressionCodec,
            erasureCodec);
    }
};

INodeTypeHandlerPtr CreateFileTypeHandler(TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

