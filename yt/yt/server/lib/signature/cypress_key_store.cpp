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

class TCypressKeyReader
    : public IKeyStoreReader
{
public:
    TCypressKeyReader(TCypressKeyReaderConfigPtr config, NApi::IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId) override
    {
        auto keyNodePath = MakeCypressKeyPath(Config_->Path, ownerId, keyId);

        TGetNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = *Config_->CypressReadOptions;
        auto result = Client_->GetNode(keyNodePath, options);

        return result.ApplyUnique(BIND([] (TYsonString&& str) mutable {
            return ConvertTo<TKeyInfoPtr>(std::move(str));
        }));
    }

private:
    const TCypressKeyReaderConfigPtr Config_;
    const IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReader)

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyWriter
    : public IKeyStoreWriter
{
public:
    TCypressKeyWriter(TCypressKeyWriterConfigPtr config, IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    //! Initialize() should be called before all other calls.
    TFuture<void> Initialize()
    {
        TCreateNodeOptions options;
        options.IgnoreExisting = true;

        auto ownerNodePath = YPathJoin(Config_->Path, ToYPathLiteral(Config_->OwnerId));

        YT_LOG_INFO("Initializing Cypress key writer (OwnerNodePath: %v)", ownerNodePath);

        return Client_->CreateNode(
            ownerNodePath,
            EObjectType::MapNode,
            options).AsVoid();
    }

    const TOwnerId& GetOwner() override
    {
        return Config_->OwnerId;
    }

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) override
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

private:
    const TCypressKeyWriterConfigPtr Config_;
    const IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriter)

} // namespace

////////////////////////////////////////////////////////////////////////////////

IKeyStoreReaderPtr CreateCypressKeyReader(TCypressKeyReaderConfigPtr config, IClientPtr client)
{
    return New<TCypressKeyReader>(std::move(config), std::move(client));
}

TFuture<IKeyStoreWriterPtr> CreateCypressKeyWriter(
    TCypressKeyWriterConfigPtr config,
    IClientPtr client)
{
    auto writer = New<TCypressKeyWriter>(std::move(config), std::move(client));
    return writer->Initialize().Apply(BIND([writer = std::move(writer)] () mutable -> IKeyStoreWriterPtr {
        return std::move(writer);
    }));
}

////////////////////////////////////////////////////////////////////////////////

TYPath MakeCypressKeyPath(const TYPath& prefixPath, const TOwnerId& ownerId, const TKeyId& keyId)
{
    return YPathJoin(prefixPath, ToYPathLiteral(ownerId), ToYPathLiteral(keyId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
