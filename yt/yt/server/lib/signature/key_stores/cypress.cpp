#include "cypress.h"

#include <yt/yt/server/lib/signature/key_info.h>

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

TFuture<TKeyInfoPtr> TCypressKeyReader::FindKey(const TOwnerId& owner, const TKeyId& key)
{
    auto keyNodePath = GetCypressKeyPath(owner, key);
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

TFuture<void> TCypressKeyWriter::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto [owner, id] = std::visit([] (const auto& meta) {
        return std::pair{meta.Owner, meta.Id};
    }, keyInfo->Meta());

    // We should not register keys belonging to other owners.
    YT_VERIFY(owner == Owner_);

    auto keyNodePath = GetCypressKeyPath(owner, id);

    auto keyExpirationTime = std::visit([] (const auto& meta) {
        return meta.ExpiresAt;
    }, keyInfo->Meta());

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("expiration_time", keyExpirationTime + KeyExpirationMargin);

    TCreateNodeOptions options;
    options.Attributes = attributes;
    auto node = Client_->CreateNode(
        keyNodePath,
        EObjectType::Document,
        std::move(options));

    return node.Apply(
        BIND([
                this,
                keyNodePath = std::move(keyNodePath),
                &keyInfo,
                this_ = MakeStrong(this)
            ] (NCypressClient::TNodeId /*nodeId*/) {
                return Client_->SetNode(keyNodePath, ConvertToYsonString(keyInfo));
            }));
}

////////////////////////////////////////////////////////////////////////////////

TYPath GetCypressKeyPath(const TOwnerId& owner, const TKeyId& key)
{
    return YPathJoin(KeyStorePath, ToYPathLiteral(owner), ToYPathLiteral(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
