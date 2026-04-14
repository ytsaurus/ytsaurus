#include "cypress_key_store.h"

#include "config.h"
#include "key_info.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NThreading;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TYPath MakeCypressKeyPath(const TYPath& prefixPath, const TOwnerId& ownerId, const TKeyId& keyId)
{
    return YPathJoin(prefixPath, ToYPathLiteral(ownerId), ToYPathLiteral(keyId));
}

TYPath MakeCypressOwnerPath(const TYPath& prefixPath, const TOwnerId& ownerId)
{
    return YPathJoin(prefixPath, ToYPathLiteral(ownerId));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TCypressKeyReader(TCypressKeyReaderConfigPtr config, IClientPtr client)
    : Config_(std::move(config))
    , Client_(std::move(client))
{ }

void TCypressKeyReader::Reconfigure(TCypressKeyReaderConfigPtr config)
{
    YT_VERIFY(config);
    Config_.Store(std::move(config));
    YT_LOG_INFO("Cypress key reader reconfigured");
}

TFuture<TKeyInfoPtr> TCypressKeyReader::FindKey(const TOwnerId& ownerId, const TKeyId& keyId) const
{
    auto config = Config_.Acquire();
    auto keyNodePath = MakeCypressKeyPath(config->Path, ownerId, keyId);

    YT_LOG_DEBUG("Looking for public key in Cypress (OwnerId: %v, KeyId: %v, Path: %v)",
        ownerId,
        keyId,
        keyNodePath);

    TGetNodeOptions options;
    static_cast<TMasterReadOptions&>(options) = *config->CypressReadOptions;
    auto result = Client_->GetNode(keyNodePath, options);

    return result.AsUnique().Apply(BIND([] (TYsonString&& str) {
        auto keyInfo = ConvertTo<TKeyInfoPtr>(std::move(str));
        auto [ownerId, keyId] = std::visit([] (const auto& meta) {
            return std::pair(meta.OwnerId, meta.KeyId);
        }, keyInfo->Meta());
        YT_LOG_DEBUG("Found public key in Cypress (OwnerId: %v, KeyId: %v)",
            ownerId,
            keyId);
        return std::move(keyInfo);
    }));
}

////////////////////////////////////////////////////////////////////////////////

TCypressKeyWriter::TCypressKeyWriter(TCypressKeyWriterConfigPtr config, TOwnerId ownerId, NNative::IClientPtr client)
    : Config_(std::move(config))
    , OwnerId_(ownerId)
    , Client_(std::move(client))
{ }

void TCypressKeyWriter::Reconfigure(TCypressKeyWriterConfigPtr config)
{
    YT_VERIFY(config);
    Config_.Store(std::move(config));
    YT_LOG_INFO("Cypress key writer reconfigured");
}

TOwnerId TCypressKeyWriter::GetOwner() const
{
    return OwnerId_;
}

TFuture<void> TCypressKeyWriter::CleanUpKeysIfLimitReached(TCypressKeyWriterConfigPtr config)
{
    YT_VERIFY(config->MaxKeyCount);
    auto ownerNodePath = MakeCypressOwnerPath(config->Path, OwnerId_);
    TGetNodeOptions options;
    options.Attributes = {"expiration_time"};
    return Client_->GetNode(ownerNodePath, options)
        .Apply(BIND([
                this,
                this_ = MakeStrong(this),
                config = std::move(config)
            ] (const TErrorOr<TYsonString>& result) mutable {
                return DoCleanUpOnLimitReached(config, result);
            }));
}

TFuture<void> TCypressKeyWriter::DoCleanUpOnLimitReached(const TCypressKeyWriterConfigPtr& config, const TErrorOr<TYsonString>& ownerNode)
{
    if (!ownerNode.IsOK()) {
        if (ownerNode.FindMatching(NYTree::EErrorCode::ResolveError)) {
            YT_LOG_ERROR(ownerNode, "Skipping cleaning up keys: owner node does not exist");
            return OKFuture;
        }
        return MakeFuture(TError(ownerNode));
    }

    auto currentKeys = ConvertTo<IMapNodePtr>(ownerNode.Value());
    auto currentKeyCount = currentKeys->GetChildCount();
    if (currentKeyCount + 1 <= *config->MaxKeyCount) {
        YT_LOG_DEBUG("Skipping cleaning up keys (CurrentKeyCount: %v, MaxKeyCount: %v)",
            currentKeyCount,
            *config->MaxKeyCount);
        return OKFuture;
    }

    int keysToDelete = currentKeyCount + 1 - *config->MaxKeyCount;
    YT_LOG_WARNING(
        "Current key count exceeds maximum allowed per owner, cleaning up keys with nearest expiration (CurrentKeyCount: %v, MaxKeyCount: %v, ToDelete: %v)",
        currentKeyCount,
        config->MaxKeyCount,
        keysToDelete);

    std::vector<std::pair<TInstant, TKeyId>> sortedKeys;
    sortedKeys.reserve(currentKeyCount);
    for (const auto& [keyId, keyNode] : currentKeys->GetChildren()) {
        auto expirationTime = keyNode->Attributes().Get<TInstant>("expiration_time");
        sortedKeys.emplace_back(expirationTime, ConvertTo<TKeyId>(keyId));
    }
    std::ranges::nth_element(sortedKeys, sortedKeys.begin() + keysToDelete);

    auto proxy = CreateObjectServiceWriteProxy(Client_);
    auto batchReq = proxy.ExecuteBatch();
    for (const auto& [_, keyId] : sortedKeys | std::views::take(keysToDelete)) {
        auto req = TCypressYPathProxy::Remove(MakeCypressKeyPath(config->Path, OwnerId_, TKeyId(keyId)));
        req->set_force(true);
        batchReq->AddRequest(req);
    }
    return batchReq->Invoke().AsVoid();
}

TFuture<void> TCypressKeyWriter::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto [ownerId, keyId] = std::visit([] (const auto& meta) {
        return std::pair(meta.OwnerId, meta.KeyId);
    }, keyInfo->Meta());

    YT_LOG_DEBUG("Registering key (OwnerId: %v, KeyId: %v)",
        ownerId,
        keyId);

    auto config = Config_.Acquire();

    // We should not register keys belonging to other owners.
    YT_VERIFY(ownerId == OwnerId_);

    auto cleanUpFuture = config->MaxKeyCount ? CleanUpKeysIfLimitReached(config) : OKFuture;
    return cleanUpFuture
        .Apply(BIND([
                this,
                this_ = MakeStrong(this),
                config = std::move(config),
                keyInfo = keyInfo,
                ownerId = std::move(ownerId),
                keyId = std::move(keyId)
            ] () mutable {
                YT_LOG_DEBUG("Cleanup finished, registering key (OwnerId: %v, KeyId: %v)",
                    ownerId,
                    keyId);
                return DoRegisterKey(config, std::move(keyInfo), std::move(ownerId), std::move(keyId));
            }));
}

TFuture<void> TCypressKeyWriter::DoRegisterKey(const TCypressKeyWriterConfigPtr& config, TKeyInfoPtr keyInfo, TOwnerId ownerId, TKeyId keyId)
{
    auto ownerNodePath = MakeCypressOwnerPath(config->Path, ownerId);
    auto keyNodePath = MakeCypressKeyPath(config->Path, ownerId, keyId);

    auto keyExpirationTime = std::visit([] (const auto& meta) {
        return meta.ExpiresAt;
    }, keyInfo->Meta());

    TCreateNodeOptions options;
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("expiration_time", keyExpirationTime + config->KeyDeletionDelay);
    options.Attributes = std::move(attributes);
    options.Recursive = true;
    return Client_->CreateNode(keyNodePath, EObjectType::Document, options)
        .Apply(BIND([this, this_ = MakeStrong(this), keyInfoYson = ConvertToYsonString(keyInfo), keyNodePath = std::move(keyNodePath)] (TNodeId /*nodeId*/) {
            return Client_->SetNode(keyNodePath, keyInfoYson);
        }))
        .Apply(BIND([ownerId = std::move(ownerId), keyId = std::move(keyId)] (const TError& error) {
            if (!error.IsOK()) {
                return error.Wrap("Failed to register key")
                    << TErrorAttribute("owner_id", ownerId)
                    << TErrorAttribute("key_id", keyId);
            }
            YT_LOG_DEBUG("Successfully registered key (OwnerId: %v, KeyId: %v)",
                ownerId,
                keyId);
            return TError();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
