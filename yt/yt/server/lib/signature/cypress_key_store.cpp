#include "cypress_key_store.h"

#include "config.h"
#include "key_info.h"
#include "key_store.h"
#include "private.h"

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
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

TYPath MakeCypressKeyPath(const TYPath& prefixPath, const TOwnerId& ownerId, const TKeyId& keyId)
{
    return YPathJoin(prefixPath, ToYPathLiteral(ownerId), ToYPathLiteral(keyId));
}

TFuture<void> DoInitializeNode(const NApi::IClientPtr& client, const TCypressKeyWriterConfigPtr& config)
{
    TCreateNodeOptions options;
    options.IgnoreExisting = true;

    auto ownerNodePath = YPathJoin(config->Path, ToYPathLiteral(config->OwnerId));

    YT_LOG_INFO("Initializing Cypress key writer (OwnerNodePath: %v)", ownerNodePath);

    return client->CreateNode(
        ownerNodePath,
        EObjectType::MapNode,
        options).AsVoid();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TCypressKeyReader(TCypressKeyReaderConfigPtr config, NApi::IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
{ }

TFuture<TKeyInfoPtr> TCypressKeyReader::FindKey(const TOwnerId& ownerId, const TKeyId& keyId) const
{
    auto keyNodePath = MakeCypressKeyPath(Config_->Path, ownerId, keyId);

    YT_LOG_DEBUG("Looking for public key in Cypress (OwnerId: %v, KeyId: %v, Path: %v)", ownerId, keyId, keyNodePath);

    TGetNodeOptions options;
    static_cast<TMasterReadOptions&>(options) = *Config_->CypressReadOptions;
    auto result = Client_->GetNode(keyNodePath, options);

    return result.ApplyUnique(BIND([] (TYsonString&& str) {
        auto keyInfo = ConvertTo<TKeyInfoPtr>(std::move(str));
        auto [ownerId, keyId] = std::visit([] (const auto& meta) {
            return std::pair(meta.OwnerId, meta.KeyId);
        }, keyInfo->Meta());
        YT_LOG_DEBUG("Found public key in Cypress (OwnerId: %v, KeyId: %v)", ownerId, keyId);
        return std::move(keyInfo);
    }));
}


////////////////////////////////////////////////////////////////////////////////

TCypressKeyWriter::TCypressKeyWriter(TCypressKeyWriterConfigPtr config, IClientPtr client)
    : Config_(std::move(config))
    , Client_(std::move(client))
{ }

//! Initialize() should be called before all other calls.
TFuture<void> TCypressKeyWriter::Initialize()
{
    return DoInitializeNode(Client_, Config_);
}

////////////////////////////////////////////////////////////////////////////////

const TOwnerId& TCypressKeyWriter::GetOwner() const
{
    return Config_->OwnerId;
}

TFuture<void> TCypressKeyWriter::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto [ownerId, keyId] = std::visit([] (const auto& meta) {
        return std::pair(meta.OwnerId, meta.KeyId);
    }, keyInfo->Meta());

    YT_LOG_DEBUG("Registering key (OwnerId: %v, KeyId: %v)", ownerId, keyId);

    // We should not register keys belonging to other owners.
        YT_VERIFY(ownerId == Config_->OwnerId);

        auto keyNodePath = MakeCypressKeyPath(Config_->Path, ownerId, keyId);

    auto keyExpirationTime = std::visit([] (const auto& meta) {
        return meta.ExpiresAt;
    }, keyInfo->Meta());

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("expiration_time", keyExpirationTime + Config_->KeyDeletionDelay);

    // TODO(pavook) retrying channel.
    TCreateNodeOptions options;
    options.Attributes = attributes;
    auto node = Client_->CreateNode(
        keyNodePath,
        EObjectType::Document,
        options);

    return node.ApplyUnique(
        BIND([
                this,
                keyNodePath = std::move(keyNodePath),
                keyInfo,
                this_ = MakeStrong(this)
            ] (NCypressClient::TNodeId&& /*nodeId*/) mutable {
                return Client_->SetNode(
                    keyNodePath,
                    ConvertToYsonString(std::move(keyInfo)));
            }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TCypressKeyWriterPtr> CreateCypressKeyWriter(
    TCypressKeyWriterConfigPtr config,
    IClientPtr client)
{
    auto writer = New<TCypressKeyWriter>(std::move(config), std::move(client));
    return writer->Initialize().Apply(BIND([writer = std::move(writer)] () mutable {
        return std::move(writer);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
