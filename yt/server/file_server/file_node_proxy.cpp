#include "file_node_proxy.h"
#include "private.h"
#include "file_node.h"

#include <yt/server/chunk_server/chunk_owner_node_proxy.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NFileClient;
using namespace NObjectServer;
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
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TFileNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TFileNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl<TFileNode>();

        descriptors->push_back(TAttributeDescriptor("executable")
            .SetCustom(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("file_name")
            .SetCustom(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("md5")
            .SetPresent(node->GetMD5Hasher().HasValue())
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer)
    {
        const auto* node = GetThisImpl<TFileNode>();

        if (key == "md5") {
            auto hasher = node->GetMD5Hasher();
            if (hasher) {
                BuildYsonFluently(consumer)
                    .Value(hasher->GetHexDigestLower());
                return true;
            }
            return false;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual void ValidateCustomAttributeUpdate(
        const TString& key,
        const TYsonString& oldValue,
        const TYsonString& newValue) override
    {
        if (key == "executable" && newValue) {
            ConvertTo<bool>(newValue);
            return;
        }

        if (key == "file_name" && newValue) {
            ConvertTo<TString>(newValue);
            return;
        }

        TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
    }

    virtual void ValidateFetchParameters(const std::vector<NChunkClient::TReadRange>& ranges) override
    {
        for (const auto& range : ranges) {
            const auto& lowerLimit = range.LowerLimit();
            const auto& upperLimit = range.UpperLimit();
            if (upperLimit.HasKey() || lowerLimit.HasKey()) {
                THROW_ERROR_EXCEPTION("Key selectors are not supported for files");
            }
            if (lowerLimit.HasRowIndex() || upperLimit.HasRowIndex()) {
                THROW_ERROR_EXCEPTION("Row index selectors are not supported for files");
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateFileNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TFileNode* trunkNode)
{
    return New<TFileNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
