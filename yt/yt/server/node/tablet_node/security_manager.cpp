#include "security_manager.h"
#include "bootstrap.h"
#include "private.h"
#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NTabletClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

struct TResourceLimitsKey
{
    TString Account;
    TString MediumName;
    std::optional<TString> TabletCellBundle;
    EInMemoryMode InMemoryMode;

    // Hasher.
    operator size_t() const
    {
        return MultiHash(
            Account,
            TabletCellBundle,
            MediumName,
            InMemoryMode);
    }

    // Comparer.
    bool operator == (const TResourceLimitsKey& other) const
    {
        return
            Account == other.Account &&
            MediumName == other.MediumName &&
            TabletCellBundle == other.TabletCellBundle &&
            InMemoryMode == other.InMemoryMode;
    }

    // Formatter.
    friend TString ToString(const TResourceLimitsKey& key)
    {
        return Format("%v:%v:%v:%v",
            key.Account,
            key.MediumName,
            key.TabletCellBundle,
            key.InMemoryMode);
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TResourceLimitsCache)

class TResourceLimitsCache
    : public TAsyncExpiringCache<TResourceLimitsKey, void>
{
public:
    TResourceLimitsCache(
        TAsyncExpiringCacheConfigPtr config,
        NCellarNode::IBootstrap* bootstrap)
        : TAsyncExpiringCache(
            std::move(config),
            TabletNodeLogger.WithTag("Cache: ResourceLimits"))
        , Bootstrap_(bootstrap)
    { }

private:
    NCellarNode::IBootstrap* const Bootstrap_;

    TFuture<void> DoGet(const TResourceLimitsKey& key, bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_LOG_DEBUG("Resource limits violation check started (Key: %v)",
            key);

        auto client = Bootstrap_->GetClient();
        auto options = TGetNodeOptions();
        options.ReadFrom = EMasterChannelKind::Cache;

        auto config = GetConfig();
        options.ExpireAfterSuccessfulUpdateTime = config->ExpireAfterSuccessfulUpdateTime;
        options.ExpireAfterFailedUpdateTime = config->ExpireAfterFailedUpdateTime;

        auto accountTabletStaticValidationEnabledAsync = client->GetNode(
            "//sys/@config/security_manager/enable_tablet_resource_validation",
            options);
        auto bundleTabletStaticValidationEnabledAsync = client->GetNode(
            "//sys/@config/tablet_manager/enable_tablet_resource_validation",
            options);
        auto accountLimitsAsync = client->GetNode(
            "//sys/accounts/" + ToYPathLiteral(key.Account) + "/@violated_resource_limits",
            options);
        auto bundleLimitsAsync = key.TabletCellBundle
            ? client->GetNode(
                "//sys/tablet_cell_bundles/" + ToYPathLiteral(*key.TabletCellBundle) + "/@violated_resource_limits",
                options)
            : MakeFuture<TYsonString>({});

        std::vector<TFuture<TYsonString>> futures{
            std::move(accountTabletStaticValidationEnabledAsync),
            std::move(bundleTabletStaticValidationEnabledAsync),
            std::move(accountLimitsAsync),
            std::move(bundleLimitsAsync)};

        return AllSet(futures).Apply(BIND(
            [key, this, this_ = MakeStrong(this)] (const std::vector<TErrorOr<TYsonString>>& rspsOrErrors) {
                bool validateAccountTabletStatic = ExtractBoolUnlessResolveError(rspsOrErrors[0], true);
                bool validateBundleTabletStatic = ExtractBoolUnlessResolveError(rspsOrErrors[1], false);
                DoValidateAccountLimits(rspsOrErrors[2], key, validateAccountTabletStatic);
                if (key.TabletCellBundle && validateBundleTabletStatic) {
                    DoValidateBundleLimits(rspsOrErrors[3], key);
                }
            }));
    }

    static bool ExtractBoolUnlessResolveError(const TErrorOr<TYsonString>& rspOrError, bool defaultValue)
    {
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return defaultValue;
        }
        const auto& value = rspOrError.ValueOrThrow();
        return ConvertTo<bool>(value);
    }

    void DoValidateAccountLimits(
        const TErrorOr<TYsonString>& resultOrError,
        const TResourceLimitsKey& key,
        bool validateTabletStaticMemory)
    {
        if (!resultOrError.IsOK()) {
            auto wrappedError = TError("Error getting resource limits for account %Qv",
                key.Account)
                << resultOrError;
            YT_LOG_WARNING(wrappedError);
            THROW_ERROR wrappedError;
        }

        const auto& node = ConvertToNode(resultOrError.Value());

        YT_LOG_DEBUG("Account limits violation check completed (Key: %v, Result: %v)",
            key.Account,
            ConvertToYsonString(node, EYsonFormat::Text));

        if (node->AsMap()->GetChildValueOrThrow<bool>("chunk_count")) {
            THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AccountLimitExceeded,
                "Account %Qv violates chunk count limit",
                key.Account);
        }

        if (key.InMemoryMode != EInMemoryMode::None && validateTabletStaticMemory) {
            if (node->AsMap()->GetChildValueOrThrow<bool>("tablet_static_memory")) {
                THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AccountLimitExceeded,
                    "Account %Qv violates tablet static memory limit",
                    key.Account);
            }
        }

        const auto& mediumLimit = node->AsMap()->GetChildOrThrow("disk_space_per_medium")->AsMap()->FindChild(key.MediumName);

        if (!mediumLimit) {
            THROW_ERROR_EXCEPTION("Unknown medium %Qv",
                key.MediumName);
        }
        if (mediumLimit->GetValue<bool>()) {
            THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AccountLimitExceeded,
                "Account %Qv violates disk space limit for medium %Qv",
                key.Account,
                key.MediumName);
        }
    }

    void DoValidateBundleLimits(
        const TErrorOr<TYsonString>& resultOrError,
        const TResourceLimitsKey& key)
    {
        if (!resultOrError.IsOK()) {
            auto wrappedError = TError("Error getting resource limits for tablet cell bundle %Qv",
                key.TabletCellBundle)
                << resultOrError;
            YT_LOG_WARNING(wrappedError);
            THROW_ERROR wrappedError;
        }

        const auto& node = ConvertToNode(resultOrError.Value());

        YT_LOG_DEBUG("Tablet cell bundle limits violation check completed (Key: %v, Result: %v)",
            key.TabletCellBundle,
            ConvertToYsonString(node, EYsonFormat::Text));

        if (key.InMemoryMode != EInMemoryMode::None) {
            if (node->AsMap()->GetChildValueOrThrow<bool>("tablet_static_memory")) {
                THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::BundleResourceLimitExceeded,
                    "Tablet cell bundle %Qv violates tablet static memory limit",
                    key.TabletCellBundle);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsCache)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSecurityManagerConfigPtr config,
        NCellarNode::IBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , ResourceLimitsCache_(New<TResourceLimitsCache>(Config_->ResourceLimitsCache, Bootstrap_))
    { }

    TFuture<void> CheckResourceLimits(
        const TString& account,
        const TString& mediumName,
        const std::optional<TString>& tabletCellBundle,
        EInMemoryMode inMemoryMode)
    {
        return ResourceLimitsCache_->Get(TResourceLimitsKey{
            account,
            mediumName,
            tabletCellBundle,
            inMemoryMode
        });
    }

    void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        const std::optional<TString>& tabletCellBundle,
        EInMemoryMode inMemoryMode)
    {
        auto asyncResult = CheckResourceLimits(account, mediumName, tabletCellBundle, inMemoryMode);
        auto optionalResult = asyncResult.TryGet();
        auto result = optionalResult ? *optionalResult : WaitFor(asyncResult);
        result.ThrowOnError();
    }

    void Reconfigure(const TSecurityManagerDynamicConfigPtr& config)
    {
        ResourceLimitsCache_->Reconfigure(config->ResourceLimitsCache ? config->ResourceLimitsCache : Config_->ResourceLimitsCache);
    }

private:
    const TSecurityManagerConfigPtr Config_;
    NCellarNode::IBootstrap* const Bootstrap_;

    const TResourceLimitsCachePtr ResourceLimitsCache_;
};

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    NCellarNode::IBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        std::move(config),
        bootstrap))
{ }

TSecurityManager::~TSecurityManager() = default;

TFuture<void> TSecurityManager::CheckResourceLimits(
    const TString& account,
    const TString& mediumName,
    const std::optional<TString>& tabletCellBundle,
    EInMemoryMode inMemoryMode)
{
    return Impl_->CheckResourceLimits(account, mediumName, tabletCellBundle, inMemoryMode);
}

void TSecurityManager::ValidateResourceLimits(
    const TString& account,
    const TString& mediumName,
    const std::optional<TString>& tabletCellBundle,
    EInMemoryMode inMemoryMode)
{
    Impl_->ValidateResourceLimits(account, mediumName, tabletCellBundle, inMemoryMode);
}

void TSecurityManager::Reconfigure(const TSecurityManagerDynamicConfigPtr& config)
{
    Impl_->Reconfigure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
