#include "user_proxy.h"
#include "account.h"
#include "network_project.h"
#include "security_manager.h"
#include "subject_proxy_detail.h"
#include "user.h"

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/security_client/proto/user_ypath.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TUserProxy
    : public TSubjectProxy<TUser>
{
public:
    TUserProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TUser* user)
        : TBase(bootstrap, metadata, user)
    { }

private:
    typedef TSubjectProxy<TUser> TBase;

    virtual void ValidateRemoval() override
    {
        const auto* user = GetThisImpl();
        if (user->IsBuiltin())  {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in user %Qv",
                user->GetName());
        }
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Banned)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadRequestRateLimit)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteRequestRateLimit)
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
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        auto* user = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Banned:
                BuildYsonFluently(consumer)
                    .Value(user->GetBanned());
                return true;

            case EInternedAttributeKey::ReadRequestRateLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestRateLimit(EUserWorkloadType::Read, NObjectClient::InvalidCellTag));
                return true;

            case EInternedAttributeKey::WriteRequestRateLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestRateLimit(EUserWorkloadType::Write, NObjectClient::InvalidCellTag));
                return true;

            case EInternedAttributeKey::RequestQueueSizeLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestQueueSizeLimit(NObjectClient::InvalidCellTag));
                return true;

            case EInternedAttributeKey::RequestLimits:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestLimits());
                return true;

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

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* user = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        switch (key) {
            case EInternedAttributeKey::Banned: {
                auto banned = ConvertTo<bool>(value);
                securityManager->SetUserBanned(user, banned);
                return true;
            }

            case EInternedAttributeKey::ReadRequestRateLimit: {
                auto limit = ConvertTo<double>(value);
                if (limit < 0) {
                    THROW_ERROR_EXCEPTION("\"read_request_rate_limit\" cannot be negative");
                }
                securityManager->SetUserRequestRateLimit(user, limit, EUserWorkloadType::Read);
                return true;
            }

            case EInternedAttributeKey::WriteRequestRateLimit: {
                auto limit = ConvertTo<double>(value);
                if (limit < 0) {
                    THROW_ERROR_EXCEPTION("\"write_request_rate_limit\" cannot be negative");
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
                auto config = ConvertTo<TUserRequestLimitsConfigPtr>(value);
                securityManager->SetUserRequestLimits(user, config);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

IObjectProxyPtr CreateUserProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TUser* user)
{
    return New<TUserProxy>(bootstrap, metadata, user);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

