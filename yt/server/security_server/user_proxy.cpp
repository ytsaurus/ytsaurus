#include "user_proxy.h"
#include "account.h"
#include "security_manager.h"
#include "subject_proxy_detail.h"
#include "user.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/security_client/user_ypath.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NSecurityServer {

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
    }

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Banned)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RequestRateLimit)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RequestQueueSizeLimit)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(EInternedAttributeKey::AccessTime);
        descriptors->push_back(EInternedAttributeKey::RequestCount);
        descriptors->push_back(EInternedAttributeKey::ReadRequestTime);
        descriptors->push_back(EInternedAttributeKey::WriteRequestTime);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellStatistics)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UsableAccounts)
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

            case EInternedAttributeKey::RequestRateLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestRateLimit());
                return true;

            case EInternedAttributeKey::RequestQueueSizeLimit:
                BuildYsonFluently(consumer)
                    .Value(user->GetRequestQueueSizeLimit());
                return true;

            case EInternedAttributeKey::AccessTime:
                BuildYsonFluently(consumer)
                    .Value(user->ClusterStatistics().AccessTime);
                return true;

            case EInternedAttributeKey::RequestCount:
                BuildYsonFluently(consumer)
                    .Value(user->ClusterStatistics().RequestCount);
                return true;

            case EInternedAttributeKey::ReadRequestTime:
                BuildYsonFluently(consumer)
                    .Value(user->ClusterStatistics().ReadRequestTime);
                return true;

            case EInternedAttributeKey::WriteRequestTime:
                BuildYsonFluently(consumer)
                    .Value(user->ClusterStatistics().WriteRequestTime);
                return true;

            case EInternedAttributeKey::MulticellStatistics:
                BuildYsonFluently(consumer)
                    .DoMapFor(user->MulticellStatistics(), [] (TFluentMap fluent, const std::pair<TCellTag, const TUserStatistics&>& pair) {
                        fluent.Item(ToString(pair.first)).Value(pair.second);
                    });
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

            case EInternedAttributeKey::RequestRateLimit: {
                auto limit = ConvertTo<double>(value);
                if (limit < 0) {
                    THROW_ERROR_EXCEPTION("\"request_rate_limit\" cannot be negative");
                }
                securityManager->SetUserRequestRateLimit(user, limit);
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

} // namespace NSecurityServer
} // namespace NYT

