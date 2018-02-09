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

        descriptors->push_back(TAttributeDescriptor("banned")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("request_rate_limit")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("request_queue_size_limit")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back("access_time");
        descriptors->push_back("request_count");
        descriptors->push_back("read_request_time");
        descriptors->push_back("write_request_time");
        descriptors->push_back(TAttributeDescriptor("multicell_statistics")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("usable_accounts")
            .SetOpaque(true));
    }

    virtual bool GetBuiltinAttribute(const TString& key, NYson::IYsonConsumer* consumer) override
    {
        auto* user = GetThisImpl();

        if (key == "banned") {
            BuildYsonFluently(consumer)
                .Value(user->GetBanned());
            return true;
        }

        if (key == "request_rate_limit") {
            BuildYsonFluently(consumer)
                .Value(user->GetRequestRateLimit());
            return true;
        }

        if (key == "request_queue_size_limit") {
            BuildYsonFluently(consumer)
                .Value(user->GetRequestQueueSizeLimit());
            return true;
        }

        if (key == "access_time") {
            BuildYsonFluently(consumer)
                .Value(user->ClusterStatistics().AccessTime);
            return true;
        }

        if (key == "request_count") {
            BuildYsonFluently(consumer)
                .Value(user->ClusterStatistics().RequestCount);
            return true;
        }

        if (key == "read_request_time") {
            BuildYsonFluently(consumer)
                .Value(user->ClusterStatistics().ReadRequestTime);
            return true;
        }

        if (key == "write_request_time") {
            BuildYsonFluently(consumer)
                .Value(user->ClusterStatistics().WriteRequestTime);
            return true;
        }

        if (key == "multicell_statistics") {
            BuildYsonFluently(consumer)
                .DoMapFor(user->MulticellStatistics(), [] (TFluentMap fluent, const std::pair<TCellTag, const TUserStatistics&>& pair) {
                    fluent.Item(ToString(pair.first)).Value(pair.second);
                });
            return true;
        }

        if (key == "usable_accounts") {
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

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        auto* user = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        if (key == "banned") {
            auto banned = ConvertTo<bool>(value);
            securityManager->SetUserBanned(user, banned);
            return true;
        }

        if (key == "request_rate_limit") {
            auto limit = ConvertTo<double>(value);
            if (limit < 0) {
                THROW_ERROR_EXCEPTION("\"request_rate_limit\" cannot be negative");
            }
            securityManager->SetUserRequestRateLimit(user, limit);
            return true;
        }

        if (key == "request_queue_size_limit") {
            auto limit = ConvertTo<int>(value);
            if (limit < 0) {
                THROW_ERROR_EXCEPTION("\"request_queue_size_limit\" cannot be negative");
            }
            securityManager->SetUserRequestQueueSizeLimit(user, limit);
            return true;
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

