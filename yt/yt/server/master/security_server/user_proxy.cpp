#include "user_proxy.h"
#include "account.h"
#include "network_project.h"
#include "security_manager.h"
#include "subject_proxy_detail.h"
#include "user.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/yt/server/master/cell_server/cell_bundle.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/security_server/config.h>

#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/security_client/proto/user_ypath.pb.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NConcurrency;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TUserProxy
    : public TSubjectProxy<TUser>
{
public:
    using TSubjectProxy::TSubjectProxy;

private:
    using TBase = TSubjectProxy<TUser>;

    void ValidateRemoval() override
    {
        const auto* user = GetThisImpl();
        if (user->IsBuiltin())  {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in user %Qv",
                user->GetName());
        }
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        auto* user = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto isRoot = user == securityManager->GetRootUser();
        auto chunkServiceWeightConfigPresent = user->GetChunkServiceUserRequestWeightThrottlerConfig() != nullptr;
        auto chunkServiceBytesConfigPresent = user->GetChunkServiceUserRequestBytesThrottlerConfig() != nullptr;

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Banned)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadRequestRateLimit)
            .SetPresent(!isRoot)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteRequestRateLimit)
            .SetPresent(!isRoot)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RequestQueueSizeLimit)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RequestLimits)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UsableAccounts)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UsableNetworkProjects)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UsableTabletCellBundles)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkServiceRequestWeightThrottler)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true)
            .SetPresent(chunkServiceWeightConfigPresent));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkServiceRequestBytesThrottler)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true)
            .SetPresent(chunkServiceBytesConfigPresent));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HashedPassword)
            .SetPresent(static_cast<bool>(user->HashedPassword()))
            .SetWritable(true)
            .SetRemovable(true)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PasswordSalt)
            .SetPresent(static_cast<bool>(user->PasswordSalt()))
            .SetWritable(true)
            .SetRemovable(true)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::PasswordRevision);
        descriptors->push_back(EInternedAttributeKey::LastSeenTime);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PendingRemoval)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Tags)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AllowCreateSecondaryIndices)
            .SetWritable(true)
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        auto* user = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Banned:
                BuildYsonFluently(consumer)
                    .Value(user->GetBanned());
                return true;

            case EInternedAttributeKey::ReadRequestRateLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestRateLimit(EUserWorkloadType::Read));
                return true;

            case EInternedAttributeKey::WriteRequestRateLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestRateLimit(EUserWorkloadType::Write));
                return true;

            case EInternedAttributeKey::RequestQueueSizeLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestQueueSizeLimit(NObjectClient::InvalidCellTag));
                return true;

            case EInternedAttributeKey::RequestLimits: {
                const auto& multicellManager = Bootstrap_->GetMulticellManager();

                auto userLimitsSerializer = TSerializableUserRequestLimitsConfig::CreateFrom(user->GetObjectServiceRequestLimits(), multicellManager);
                BuildYsonFluently(consumer)
                    .Value(userLimitsSerializer);

                return true;
            }

            case EInternedAttributeKey::ChunkServiceRequestWeightThrottler: {
                if (!user->GetChunkServiceUserRequestWeightThrottlerConfig()) {
                    break;
                }

                auto config = user->GetChunkServiceUserRequestWeightThrottlerConfig();
                BuildYsonFluently(consumer)
                    .Value(config);
                return true;
            }

            case EInternedAttributeKey::ChunkServiceRequestBytesThrottler: {
                if (!user->GetChunkServiceUserRequestBytesThrottlerConfig()) {
                    break;
                }

                auto config = user->GetChunkServiceUserRequestBytesThrottlerConfig();
                BuildYsonFluently(consumer)
                    .Value(config);
                return true;
            }

            case EInternedAttributeKey::UsableAccounts: {
                const auto& securityManager = Bootstrap_->GetSecurityManager();
                BuildYsonFluently(consumer)
                    .DoListFor(securityManager->Accounts(), [&] (TFluentList fluent, const std::pair<const TAccountId, TAccount*>& pair) {
                        auto* account = pair.second;

                        if (!IsObjectAlive(account)) {
                            return;
                        }

                        auto permissionCheckResult = securityManager->CheckPermission(account, user, EPermission::Use);
                        if (permissionCheckResult.Action == ESecurityAction::Allow) {
                            fluent.Item().Value(account->GetName());
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::UsableTabletCellBundles: {
                const auto& cellManager = Bootstrap_->GetTamedCellManager();
                const auto& securityManager = Bootstrap_->GetSecurityManager();
                BuildYsonFluently(consumer)
                    .DoListFor(cellManager->CellBundles(), [&] (TFluentList fluent,
                        const std::pair<const NCellServer::TCellBundleId, NCellServer::TCellBundle*>& pair) {
                        auto* cellBundle = pair.second;
                        if (!IsObjectAlive(cellBundle)) {
                            return;
                        }
                        if (cellBundle->GetType() != EObjectType::TabletCellBundle) {
                            return;
                        }
                        auto* tabletCellBundle = cellBundle->As<NTabletServer::TTabletCellBundle>();
                        auto permissionCheckResult = securityManager->CheckPermission(tabletCellBundle, user, EPermission::Use);
                        if (permissionCheckResult.Action == ESecurityAction::Allow) {
                            fluent.Item().Value(tabletCellBundle->GetName());
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::UsableNetworkProjects: {
                const auto& securityManager = Bootstrap_->GetSecurityManager();
                BuildYsonFluently(consumer)
                    .DoListFor(securityManager->NetworkProjects(), [&] (TFluentList fluent, const std::pair<const TNetworkProjectId, TNetworkProject*>& pair) {
                        auto* networkProject = pair.second;

                        if (!IsObjectAlive(networkProject)) {
                            return;
                        }

                        auto permissionCheckResult = securityManager->CheckPermission(networkProject, user, EPermission::Use);
                        if (permissionCheckResult.Action == ESecurityAction::Allow) {
                            fluent.Item().Value(networkProject->GetName());
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::PasswordRevision:
                BuildYsonFluently(consumer)
                    .Value(user->GetPasswordRevision());
                return true;

            case EInternedAttributeKey::HashedPassword:
                if (!user->HashedPassword()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(user->HashedPassword());
                return true;

            case EInternedAttributeKey::PasswordSalt:
                if (!user->PasswordSalt()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(user->PasswordSalt());
                return true;

            case EInternedAttributeKey::Tags:
                BuildYsonFluently(consumer)
                    .Value(user->Tags().GetSourceTags());
                return true;

            case EInternedAttributeKey::PendingRemoval:
                BuildYsonFluently(consumer)
                    .Value(user->GetPendingRemoval());
                return true;

            case EInternedAttributeKey::AllowCreateSecondaryIndices:
                BuildYsonFluently(consumer)
                    .Value(user->GetAllowCreateSecondaryIndices());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* user = GetThisImpl();
        switch (key) {
            case EInternedAttributeKey::LastSeenTime: {
                std::vector<TFuture<TYPathProxy::TRspGetPtr>> asyncResults;
                if (IsPrimaryMaster()) {
                    const auto& multicellManager = Bootstrap_->GetMulticellManager();
                    auto portalCellTags = multicellManager->GetRoleMasterCells(NCellMaster::EMasterCellRole::CypressNodeHost);

                    for (auto portalCellTag : portalCellTags) {
                        if (portalCellTag == multicellManager->GetCellTag()) {
                            continue;
                        }

                        auto proxy = CreateObjectServiceReadProxy(
                            Bootstrap_->GetRootClient(),
                            EMasterChannelKind::Follower,
                            portalCellTag);
                        asyncResults.push_back(proxy.Execute(TYPathProxy::Get(user->GetObjectPath() + "/@last_seen_time")));
                    }
                }

                return AllSucceeded(asyncResults).Apply(
                    BIND([initialLastSeenTime = user->GetLastSeenTime()] (const std::vector<TYPathProxy::TRspGetPtr>& results) {
                        auto lastSeenTime = initialLastSeenTime;
                        for (const auto& result : results) {
                            lastSeenTime = std::max(lastSeenTime, ConvertTo<TInstant>(TYsonString(result->value())));
                        }
                        return BuildYsonStringFluently().Value(lastSeenTime);
                    }));
            }

            default:
                break;
        }
        return TBase::GetBuiltinAttributeAsync(key);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* user = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* rootUser = securityManager->GetRootUser();

        switch (key) {
            case EInternedAttributeKey::Banned: {
                auto banned = ConvertTo<bool>(value);
                securityManager->SetUserBanned(user, banned);
                return true;
            }

            case EInternedAttributeKey::ReadRequestRateLimit: {
                auto limit = ConvertTo<int>(value);
                if (limit < 0) {
                    THROW_ERROR_EXCEPTION("\"read_request_rate_limit\" cannot be negative");
                }
                if (user == rootUser) {
                    THROW_ERROR_EXCEPTION("Cannot set \"read_request_rate_limit\" for %Qv",
                        user->GetName());
                }
                securityManager->SetUserRequestRateLimit(user, limit, EUserWorkloadType::Read);
                return true;
            }

            case EInternedAttributeKey::WriteRequestRateLimit: {
                auto limit = ConvertTo<int>(value);
                if (limit < 0) {
                    THROW_ERROR_EXCEPTION("\"write_request_rate_limit\" cannot be negative");
                }
                if (user == rootUser) {
                    THROW_ERROR_EXCEPTION("Cannot set \"write_request_rate_limit\" for %Qv",
                        user->GetName());
                }
                securityManager->SetUserRequestRateLimit(user, limit, EUserWorkloadType::Write);
                return true;
            }

            case EInternedAttributeKey::RequestQueueSizeLimit: {
                auto limit = ConvertTo<int>(value);
                if (limit < 0) {
                    THROW_ERROR_EXCEPTION("\"request_queue_size_limit\" cannot be negative");
                }
                securityManager->SetUserRequestQueueSizeLimit(user, limit);
                return true;
            }

            case EInternedAttributeKey::RequestLimits: {
                const auto& multicellManager = Bootstrap_->GetMulticellManager();

                auto config = ConvertTo<TSerializableUserRequestLimitsConfigPtr>(value)->ToConfigOrThrow(multicellManager);
                securityManager->SetUserRequestLimits(user, config);
                return true;
            }

            case EInternedAttributeKey::ChunkServiceRequestWeightThrottler: {
                if (user == rootUser) {
                    THROW_ERROR_EXCEPTION("Cannot set %Qv for %Qv",
                        key.Unintern(),
                        user->GetName());
                }

                auto config = ConvertTo<TThroughputThrottlerConfigPtr>(value);
                securityManager->SetChunkServiceUserRequestWeightThrottlerConfig(user, config);
                return true;
            }

            case EInternedAttributeKey::ChunkServiceRequestBytesThrottler: {
                if (user == rootUser) {
                    THROW_ERROR_EXCEPTION("Cannot set %Qv for %Qv",
                        key.Unintern(),
                        user->GetName());
                }

                auto config = ConvertTo<TThroughputThrottlerConfigPtr>(value);
                securityManager->SetChunkServiceUserRequestBytesThrottlerConfig(user, config);
                return true;
            }

            case EInternedAttributeKey::HashedPassword: {
                auto hashedPassword = ConvertTo<TString>(value);
                user->SetHashedPassword(std::move(hashedPassword));
                return true;
            }

            case EInternedAttributeKey::PasswordSalt: {
                auto passwordSalt = ConvertTo<TString>(value);
                user->SetPasswordSalt(std::move(passwordSalt));
                return true;
            }

            case EInternedAttributeKey::Tags: {
                const auto& securityManagerDynconfig = Bootstrap_
                    ->GetConfigManager()
                    ->GetConfig()
                    ->SecurityManager;
                auto newTags = ConvertTo<THashSet<TString>>(value);
                if (std::ssize(newTags) > securityManagerDynconfig->MaxUserTagCount) {
                    THROW_ERROR_EXCEPTION("Cannot set user tags as user tags count limit exceeded")
                        << TErrorAttribute ("max_user_tag_count", securityManagerDynconfig->MaxUserTagCount);
                }
                for (const auto& tag: newTags) {
                    if (std::ssize(tag) >= securityManagerDynconfig->MaxUserTagSize) {
                        THROW_ERROR_EXCEPTION("Cannot set user tags as user tag size limit exceeded")
                            << TErrorAttribute("max_user_tag_size", securityManagerDynconfig->MaxUserTagSize)
                            << TErrorAttribute("tag", tag);
                    }
                    ValidateBooleanFormulaVariable(tag);
                }
                user->Tags() = TBooleanFormulaTags(std::move(newTags));
                return true;
            }

            case EInternedAttributeKey::PendingRemoval: {
                auto pendingRemoval = ConvertTo<bool>(value);
                user->SetPendingRemoval(pendingRemoval);
                return true;
            }

            case EInternedAttributeKey::AllowCreateSecondaryIndices:
                user->SetAllowCreateSecondaryIndices(ConvertTo<bool>(value));
                return true;

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* user = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        switch (key) {
            case EInternedAttributeKey::ChunkServiceRequestWeightThrottler: {
                securityManager->SetChunkServiceUserRequestWeightThrottlerConfig(user, nullptr);
                return true;
            }

            case EInternedAttributeKey::ChunkServiceRequestBytesThrottler: {
                securityManager->SetChunkServiceUserRequestBytesThrottlerConfig(user, nullptr);
                return true;
            }

            case EInternedAttributeKey::HashedPassword: {
                user->SetHashedPassword(/*hashedPassword*/ std::nullopt);
                return true;
            }

            case EInternedAttributeKey::PasswordSalt: {
                user->SetPasswordSalt(/*passwordSalt*/ std::nullopt);
                return true;
            }

            case EInternedAttributeKey::Tags: {
                user->Tags() = {};
                return true;
            }

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateUserProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TUser* user)
{
    return New<TUserProxy>(bootstrap, metadata, user);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

