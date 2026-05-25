#include "cypress_key_reader.h"

#include "config.h"

#include <yt/yt/server/lib/signature/common/key_info.h>
#include <yt/yt/server/lib/signature/common/private.h>

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

} // namespace NYT::NSignature
