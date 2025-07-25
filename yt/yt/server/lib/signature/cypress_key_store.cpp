#include "cypress_key_store.h"

#include "config.h"
#include "key_info.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

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

    YT_LOG_DEBUG("Looking for public key in Cypress (OwnerId: %v, KeyId: %v, Path: %v)", ownerId, keyId, keyNodePath);

    TGetNodeOptions options;
    static_cast<TMasterReadOptions&>(options) = *config->CypressReadOptions;
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

TCypressKeyWriter::TCypressKeyWriter(TCypressKeyWriterConfigPtr config, NNative::IClientPtr client)
    : Config_(std::move(config))
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
    return Config_.Acquire()->OwnerId;
}

TFuture<void> TCypressKeyWriter::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto [ownerId, keyId] = std::visit([] (const auto& meta) {
        return std::pair(meta.OwnerId, meta.KeyId);
    }, keyInfo->Meta());

    YT_LOG_DEBUG("Registering key (OwnerId: %v, KeyId: %v)", ownerId, keyId);

    auto config = Config_.Acquire();

    // We should not register keys belonging to other owners.
    YT_VERIFY(ownerId == config->OwnerId);

    auto ownerNodePath = MakeCypressOwnerPath(config->Path, ownerId);
    auto keyNodePath = MakeCypressKeyPath(config->Path, ownerId, keyId);

    auto keyExpirationTime = std::visit([] (const auto& meta) {
        return meta.ExpiresAt;
    }, keyInfo->Meta());

    // Use object service proxy with batch requests.
    auto proxy = CreateObjectServiceWriteProxy(Client_);

    // TODO(pavook): with retries.
    auto batchReq = proxy.ExecuteBatch();

    // Always initialize the owner node first.
    {
        auto req = TCypressYPathProxy::Create(ownerNodePath);
        req->set_type(ToProto(EObjectType::MapNode));
        req->set_ignore_existing(true);
        batchReq->AddRequest(req, "create_owner");
    }

    // Create the key document node.
    {
        auto req = TCypressYPathProxy::Create(keyNodePath);
        req->set_type(ToProto(EObjectType::Document));
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("expiration_time", keyExpirationTime + config->KeyDeletionDelay);
        ToProto(req->mutable_node_attributes(), *attributes);
        batchReq->AddRequest(req, "create_key");
    }

    // Set the key content.
    {
        auto req = TObjectYPathProxy::Set(keyNodePath);
        req->set_value(ConvertToYsonString(keyInfo).ToString());
        batchReq->AddRequest(req, "set_key_content");
    }

    return batchReq->Invoke().ApplyUnique(BIND(
        [ownerId = std::move(ownerId), keyId = std::move(keyId)] (TErrorOr<TObjectServiceProxy::TRspExecuteBatchPtr>&& batchRsp) -> TError {
            auto cumulativeError = GetCumulativeError(batchRsp);
            if (!cumulativeError.IsOK()) {
                return cumulativeError.Wrap("Failed to register key (OwnerId: %v, KeyId: %v)", ownerId, keyId);
            }

            YT_LOG_DEBUG("Successfully registered key (OwnerId: %v, KeyId: %v)", ownerId, keyId);
            return {};
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
