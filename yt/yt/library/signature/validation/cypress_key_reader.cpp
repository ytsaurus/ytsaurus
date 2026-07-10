#include "cypress_key_reader.h"

#include "config.h"

#include <yt/yt/library/signature/common/key_info.h>
#include <yt/yt/library/signature/common/private.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TCachedKeyInfo::TCachedKeyInfo(const TKeyDescriptor& key, TKeyInfoPtr keyInfo)
    : TAsyncCacheValueBase(key)
    , KeyInfo_(std::move(keyInfo))
{ }

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TKeyCache::TKeyCache(TCypressKeyReaderConfigPtr config, IClientPtr client)
    : TAsyncSlruCacheBase(config->KeyCache)
    , Config_(std::move(config))
    , Client_(std::move(client))
{ }

TFuture<TKeyInfoPtr> TCypressKeyReader::TKeyCache::FindKey(const TOwnerId& ownerId, const TKeyId& keyId)
{
    auto key = TKeyDescriptor(ownerId, keyId);
    auto cookie = BeginInsert(key);
    auto valueFuture = cookie.GetValue();

    if (cookie.IsActive()) {
        auto keyNodePath = MakeCypressKeyPath(Config_->Path, ownerId, keyId);

        YT_LOG_DEBUG(
            "Looking for public key in Cypress (OwnerId: %v, KeyId: %v, Path: %v)",
            ownerId,
            keyId,
            keyNodePath);

        TGetNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = *Config_->CypressReadOptions;

        Client_->GetNode(keyNodePath, options).AsUnique()
            .Apply(BIND([key = std::move(key)] (TYsonString&& str) {
                auto keyInfo = ConvertTo<TKeyInfoPtr>(std::move(str));
                auto [ownerId, keyId] = std::visit([] (const auto& meta) {
                    return std::pair(meta.OwnerId, meta.KeyId);
                }, keyInfo->Meta());
                YT_LOG_DEBUG(
                    "Found public key in Cypress (OwnerId: %v, KeyId: %v)",
                    ownerId,
                    keyId);
                return New<TCachedKeyInfo>(key, std::move(keyInfo));
            }))
            .Subscribe(BIND([cookie = std::move(cookie)] (const TErrorOr<TCachedKeyInfoPtr>& result) mutable {
                if (result.IsOK()) {
                    cookie.EndInsert(result.Value());
                } else {
                    // NB: Fetch errors are not cached.
                    cookie.Cancel(result);
                }
            }));
    }

    return valueFuture.Apply(BIND([] (const TCachedKeyInfoPtr& cachedKey) {
        return cachedKey->KeyInfo();
    }));
}

////////////////////////////////////////////////////////////////////////////////

TCypressKeyReader::TCypressKeyReader(TCypressKeyReaderConfigPtr config, IClientPtr client)
    : Client_(client)
    , KeyCache_(New<TKeyCache>(std::move(config), std::move(client)))
{ }

TFuture<TKeyInfoPtr> TCypressKeyReader::FindKey(const TOwnerId& ownerId, const TKeyId& keyId) const
{
    return KeyCache_.Acquire()->FindKey(ownerId, keyId);
}

void TCypressKeyReader::Reconfigure(TCypressKeyReaderConfigPtr config)
{
    YT_VERIFY(config);
    // TODO(pogorelov): Recreate the cache only if its config has changed.
    KeyCache_.Store(New<TKeyCache>(std::move(config), Client_));
    YT_LOG_INFO("Cypress key reader reconfigured");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
