#include "stdafx.h"
#include "file_node_proxy.h"
#include "file_node.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/read_limit.h>

#include <server/chunk_server/chunk_owner_node_proxy.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TFileNode>
{
public:
    TFileNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TFileNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TFileNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor("executable")
            .SetCustom(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("file_name")
            .SetCustom(true)
            .SetReplicated(true));
    }

    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override
    {
        if (key == "executable" && newValue) {
            ConvertTo<bool>(*newValue);
            return;
        }

        if (key == "file_name" && newValue) {
            ConvertTo<Stroka>(*newValue);
            return;
        }

        TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
    }

    virtual void ValidateFetchParameters(
        const TChannel& channel,
        const std::vector<TReadRange>& ranges) override
    {
        if (!channel.IsUniversal()) {
            THROW_ERROR_EXCEPTION("Column selectors are not supported for files");
        }

        for (const auto& range : ranges) {
            const auto& lowerLimit = range.LowerLimit();
            const auto& upperLimit = range.UpperLimit();
            if (upperLimit.HasKey() || upperLimit.HasRowIndex() ||
                lowerLimit.HasKey() || lowerLimit.HasRowIndex())
            {
                THROW_ERROR_EXCEPTION("Row selectors are not supported for files");
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateFileNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
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
