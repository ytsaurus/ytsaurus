#include "cypress_key_store.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TCypressKeyReader(IClientPtr client)
    : Client_(client)
{
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TKeyInfoPtr> TCypressKeyReader::GetKey(const TOwnerId& owner, const TKeyId& keyId)
{
    auto keyNodePath = GetCypressKeyPath(owner, keyId);
    auto res = Client_->GetNode(keyNodePath);

    return res.Apply(BIND([](const TYsonString& str) {
        return ConvertTo<TKeyInfoPtr>(str);
    }));
}

////////////////////////////////////////////////////////////////////////////////

TCypressKeyWriter::TCypressKeyWriter(TOwnerId owner, IClientPtr client)
    : Owner_(owner)
    , Client_(client)
{
    TCreateNodeOptions options;
    options.IgnoreExisting = true;
    auto node_id = WaitFor(Client_->CreateNode(
        YPathJoin(KeyStorePath, ToYPathLiteral(owner)),
        EObjectType::MapNode,
        std::move(options)));

    if (!node_id.IsOK()) {
        THROW_ERROR_EXCEPTION(
            "Failed to initialize cypress key writer (OwnerId: %v, Error: %v)",
            owner,
            node_id);
    }
}

////////////////////////////////////////////////////////////////////////////////

TOwnerId TCypressKeyWriter::GetOwner()
{
    return Owner_;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TCypressKeyWriter::RegisterKey(const TKeyInfo& key)
{
    auto [owner, id] = std::visit([] (const auto& meta) {
        return std::pair{meta.Owner, meta.Id};
    }, key.Meta());

    // We should not register keys belonging to other owners.
    YT_VERIFY(owner == Owner_);

    auto keyNodePath = GetCypressKeyPath(owner, id);

    auto keyExpirationTime = std::visit([] (const auto& meta) {
        return meta.ExpiresAt;
    }, key.Meta());

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("expiration_time", keyExpirationTime + KeyExpirationMargin);

    TCreateNodeOptions options;
    options.Attributes = attributes;
    auto node = Client_->CreateNode(
        keyNodePath,
        EObjectType::Document,
        std::move(options));

    return node.Apply(BIND([this, keyNodePath = std::move(keyNodePath), &key] (NCypressClient::TNodeId /*nodeId*/) {
        return Client_->SetNode(keyNodePath, ConvertToYsonString(key));
    }));
}

////////////////////////////////////////////////////////////////////////////////

TYPath GetCypressKeyPath(const TOwnerId& owner, const TKeyId& keyId)
{
    return YPathJoin(KeyStorePath, ToYPathLiteral(owner), ToYPathLiteral(keyId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
