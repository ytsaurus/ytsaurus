#include "file_node_proxy.h"
#include "private.h"
#include "file_node.h"

#include <yt/yt/server/master/chunk_server/chunk_owner_node_proxy.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/ytlib/file_client/file_chunk_writer.h>

namespace NYT::NFileServer {

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
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

private:
    using TBase = TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TFileNode>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl<TFileNode>();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Executable)
            .SetCustom(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FileName)
            .SetCustom(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MD5)
            .SetPresent(node->GetMD5Hasher().operator bool())
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl<TFileNode>();

        switch (key) {
            case EInternedAttributeKey::MD5: {
                auto hasher = node->GetMD5Hasher();
                if (hasher) {
                    BuildYsonFluently(consumer)
                        .Value(hasher->GetHexDigestLowerCase());
                    return true;
                }
                return false;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    void ValidateCustomAttributeUpdate(
        const TString& key,
        const TYsonString& oldValue,
        const TYsonString& newValue) override
    {
        auto internedKey = TInternedAttributeKey::Lookup(key);

        switch (internedKey) {
            case EInternedAttributeKey::Executable:
                if (!newValue) {
                    break;
                }
                ConvertTo<bool>(newValue);
                return;

            case EInternedAttributeKey::FileName:
                if (!newValue) {
                    break;
                }
                ConvertTo<TString>(newValue);
                return;

            default:
                break;
        }

        TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
    }

    void ValidateReadLimit(const NChunkClient::NProto::TReadLimit& readLimit) const override
    {
        if (readLimit.has_key_bound_prefix() || readLimit.has_legacy_key()) {
            THROW_ERROR_EXCEPTION("Key selectors are not supported for files");
        }
        if (readLimit.has_row_index()) {
            THROW_ERROR_EXCEPTION("Row index selectors are not supported for files");
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

} // namespace NYT::NFileServer
