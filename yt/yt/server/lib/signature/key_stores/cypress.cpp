#include "cypress.h"

#include <yt/yt/server/lib/signature/key_info.h>
#include <yt/yt/server/lib/signature/private.h>

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

static constexpr TYPathBuf DefaultKeyPath = "//sys/public_keys/by_owner";

////////////////////////////////////////////////////////////////////////////////

void TCypressKeyReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default(TYPath(DefaultKeyPath))
        .NonEmpty();
}

void TCypressKeyWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default(TYPath(DefaultKeyPath))
        .NonEmpty();

    registrar.Parameter("owner_id", &TThis::OwnerId)
        .CheckThat([] (const TOwnerId& owner) { return !owner.Underlying().empty(); });

    registrar.Parameter("key_deletion_delay", &TThis::KeyDeletionDelay)
        .Default(TDuration::Days(1))
        .GreaterThanOrEqual(TDuration::Zero());

    // TODO(pavook) implement.
    registrar.Parameter("max_key_count", &TThis::MaxKeyCount)
        .Default(100)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TCypressKeyReader(TCypressKeyReaderConfigPtr config, IClientPtr client)
    : Config_(std::move(config))
    , Client_(std::move(client))
{ }

TFuture<TKeyInfoPtr> TCypressKeyReader::FindKey(const TOwnerId& ownerId, const TKeyId& keyId)
{
    auto keyNodePath = MakeCypressKeyPath(Config_->Path, ownerId, keyId);
    auto result = Client_->GetNode(keyNodePath);

    return result.Apply(BIND([] (const TYsonString& str) {
        return ConvertTo<TKeyInfoPtr>(str);
    }));
}

////////////////////////////////////////////////////////////////////////////////

TCypressKeyWriter::TCypressKeyWriter(TCypressKeyWriterConfigPtr config, IClientPtr client)
    : Config_(std::move(config))
    , Client_(std::move(client))
{ }

TFuture<void> TCypressKeyWriter::Initialize()
{
    TCreateNodeOptions options;
    options.IgnoreExisting = true;

    Initialization_ = Client_->CreateNode(
        YPathJoin(Config_->Path, ToYPathLiteral(Config_->OwnerId)),
        EObjectType::MapNode,
        std::move(options)).AsVoid();

    YT_LOG_INFO("Initialized Cypress key writer");

    return Initialization_;
}

const TOwnerId& TCypressKeyWriter::GetOwner()
{
    return Config_->OwnerId;
}

TFuture<void> TCypressKeyWriter::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    YT_VERIFY(keyInfo);

    return Initialization_.Apply(BIND([this, keyInfo, this_ = MakeStrong(this)] () {
        return DoRegister(std::move(keyInfo));
    }));
}

TFuture<void> TCypressKeyWriter::DoRegister(const TKeyInfoPtr& keyInfo)
{
    auto [ownerId, keyId] = std::visit([] (const auto& meta) {
        return std::pair{meta.OwnerId, meta.KeyId};
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
                keyInfo,
                this_ = MakeStrong(this)
            ] (NCypressClient::TNodeId /*nodeId*/) mutable {
                return Client_->SetNode(keyNodePath, ConvertToYsonString(std::move(keyInfo)));
            }));
}

////////////////////////////////////////////////////////////////////////////////

TYPath MakeCypressKeyPath(const TYPath& prefixPath, const TOwnerId& ownerId, const TKeyId& keyId)
{
    return YPathJoin(prefixPath, ToYPathLiteral(ownerId), ToYPathLiteral(keyId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
