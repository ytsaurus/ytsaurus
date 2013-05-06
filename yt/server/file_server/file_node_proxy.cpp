#include "stdafx.h"
#include "file_node_proxy.h"
#include "file_node.h"

#include <ytlib/misc/string.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk_owner_node_proxy.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public NCypressServer::TCypressNodeProxyBase<
        TChunkOwnerNodeProxy,
        NYTree::IEntityNode,
        TFileNode>
{
public:
    TFileNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        TTransaction* transaction,
        TFileNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    typedef NCypressServer::TCypressNodeProxyBase<TChunkOwnerNodeProxy, NYTree::IEntityNode, TFileNode> TBase;

    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override
    {
        UNUSED(oldValue);

        if (key == "executable" && newValue) {
            ConvertTo<bool>(*newValue);
            return;
        }

        if (key == "file_name" && newValue) {
            // File name must be string.
            // ToDo(psushin): write more sophisticated validation.
            ConvertTo<Stroka>(*newValue);
            return;
        }

        TBase::ValidateUserAttributeUpdate(key, oldValue, newValue);
    }

    virtual NCypressClient::ELockMode GetLockMode(NChunkClient::EUpdateMode updateMode) override
    {
        return NCypressClient::ELockMode::Exclusive;
    }

    virtual void ValidatePathAttributes(
        const TNullable<NChunkClient::TChannel>& channel,
        const NChunkClient::NProto::TReadLimit& upperLimit,
        const NChunkClient::NProto::TReadLimit& lowerLimit) override
    {
        UNUSED(channel);
        UNUSED(upperLimit);
        UNUSED(lowerLimit);

        if (channel) {
            THROW_ERROR_EXCEPTION("YT files do not support reading with channels");
        }

        if (upperLimit.has_key() || upperLimit.has_row_index() ||
            lowerLimit.has_key() || lowerLimit.has_row_index())
        {
            THROW_ERROR_EXCEPTION("YT files do not support row selection");
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateFileNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TFileNode* trunkNode)
{

    return New<TFileNodeProxy>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
