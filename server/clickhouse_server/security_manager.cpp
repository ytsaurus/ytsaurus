#include "security_manager.h"

#include "private.h"
#include "bootstrap.h"
#include "config.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/ytlib/security_client/acl.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/ytree/permission.h>

#include <Poco/Net/IPAddress.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Common/Exception.h>
#include <Interpreters/Users.h>
#include <Interpreters/IUsersManager.h>

#include <util/generic/maybe.h>
#include <util/system/rwlock.h>

#include <map>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NSecurityClient;
using namespace NYson;

static const auto& Logger = ServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TUsersManager
    : public DB::IUsersManager
{
public:
    TUsersManager(TBootstrap* bootstrap, TString cliqueId)
        : Bootstrap_(bootstrap)
        , CliqueId_(std::move(cliqueId))
    { }

    void loadFromConfig(const Poco::Util::AbstractConfiguration& config) override
    {
        auto guard = TWriterGuard(SpinLock_);

        Users_.clear();
        UserTemplate_ = std::nullopt;

        if (config.has("user_template")) {
            UserTemplate_ = DB::User("template", "user_template", config);
        }
    }

    UserPtr authorizeAndGetUser(
        const std::string& userName,
        const std::string& /* password */,
        const Poco::Net::IPAddress& /* address */) const override
    {
        auto that = const_cast<TUsersManager*>(this);
        return that->GetOrRegisterUser(userName);
    }

    UserPtr getUser(const std::string& userName) const override
    {
        auto that = const_cast<TUsersManager*>(this);
        return that->GetOrRegisterUser(userName);
    }

    bool hasAccessToDatabase(const std::string& userName, const std::string& databaseName) const override
    {
        try {
            const_cast<TUsersManager*>(this)->GetOrRegisterUser(TString(userName));
            return true;
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Error while registering user (UserName: %v, DatabaseName: %v)", userName, databaseName);
            return false;
        }
    }

private:
    THashMap<TString, DB::IUsersManager::UserPtr> Users_;
    std::optional<DB::User> UserTemplate_;
    TReaderWriterSpinLock SpinLock_;

    TBootstrap* Bootstrap_;
    TString CliqueId_;

    std::optional<TSerializableAccessControlList> CurrentAcl_;
    NProfiling::TCpuInstant LastCurrentAclUpdateTime_ = 0;

    DB::IUsersManager::UserPtr GetOrRegisterUser(TStringBuf userName)
    {
        MaybeUpdateCurrentAcl();

        {
            auto guard = TReaderGuard(SpinLock_);

            auto found = Users_.find(userName);
            if (found != Users_.end()) {
                return found->second;
            }
        }

        {
            auto guard = TWriterGuard(SpinLock_);

            auto found = Users_.find(userName);
            if (found != Users_.end()) {
                return found->second;
            }

            YT_LOG_INFO("Registering new user (UserName: %v)", userName);
            ValidateUserAccess(TString(userName));

            auto user = CreateNewUserFromTemplate(userName);
            auto result = Users_.emplace(userName, user);
            YT_VERIFY(result.second);
            return user;
        }
    }

    void ValidateUserAccess(const TString& userName)
    {
        // At this point we only check if the user has access to current clique.
        // Storage layer is responsible for access control for specific tables.
        if (!Bootstrap_->GetConfig()->ValidateOperationAccess) {
            YT_LOG_DEBUG("Operation access validation disabled, allowing access to user (User: %v)", userName);
            return;
        }

        if (userName == "default") {
            YT_LOG_DEBUG("Username is default, allowing this user to register");
            return;
        }

        if (!CurrentAcl_) {
            YT_LOG_DEBUG("No operation ACL available, allowing access to user (User: %v)", userName);
            return;
        }

        NScheduler::ValidateOperationAccess(
            userName,
            TGuid::FromString(CliqueId_),
            TGuid() /* jobId */,
            EPermission::Read,
            *CurrentAcl_,
            Bootstrap_->GetRootClient(),
            Logger);
        YT_LOG_DEBUG("User access validated using master, allowing access to user (User: %v)", userName);
    }

    //! Try to update current acl if it is not fresh enough, and clear users map if it has actually changed.
    void MaybeUpdateCurrentAcl()
    {
        auto currentTime = NProfiling::GetCpuInstant();
        auto updatePeriod = NProfiling::DurationToCpuDuration(Bootstrap_->GetConfig()->OperationAclUpdatePeriod);
        {
            auto guard = TReaderGuard(SpinLock_);
            if (LastCurrentAclUpdateTime_ + updatePeriod >= currentTime) {
                return;
            }
        }
        {
            auto guard = TWriterGuard(SpinLock_);
            if (LastCurrentAclUpdateTime_ + updatePeriod >= currentTime) {
                // Somebody has updated acl before us.
                return;
            }

            auto newCurrentAcl = FetchCurrentAcl();
            YT_LOG_DEBUG("Current operation ACL fetched (Acl: %v)", ConvertToYsonString(newCurrentAcl, EYsonFormat::Text));
            LastCurrentAclUpdateTime_ = NProfiling::GetCpuInstant();
            if (CurrentAcl_ != newCurrentAcl) {
                YT_LOG_INFO("Operation ACL has changed (OldAcl: %v, NewAcl: %v)",
                    ConvertToYsonString(CurrentAcl_, EYsonFormat::Text),
                    ConvertToYsonString(newCurrentAcl, EYsonFormat::Text));
                Users_.clear();
                CurrentAcl_ = newCurrentAcl;
            }
        }
    }

    std::optional<TSerializableAccessControlList> FetchCurrentAcl()
    {
        NApi::TGetOperationOptions options;
        options.IncludeRuntime = true;
        options.Attributes = THashSet<TString>{"runtime_parameters"};
        auto runtimeParametersYsonString = WaitFor(Bootstrap_->GetRootClient()->GetOperation(
            TGuid::FromString(CliqueId_),
            std::move(options)))
            .ValueOrThrow();
        auto runtimeParametersNode = ConvertTo<IMapNodePtr>(runtimeParametersYsonString)
            ->FindChild("runtime_parameters");
        if (!runtimeParametersNode) {
            return std::nullopt;
        }
        auto aclNode = runtimeParametersNode
            ->AsMap()
            ->FindChild("acl");
        if (!aclNode) {
            return std::nullopt;
        }
        return ConvertTo<TSerializableAccessControlList>(std::move(aclNode));
    }

    DB::IUsersManager::UserPtr CreateNewUserFromTemplate(TStringBuf userName)
    {
        if (!UserTemplate_) {
            throw DB::Exception(
                "Cannot automatically register new user: user template not provided",
                DB::ErrorCodes::UNKNOWN_USER);
        };

        auto newUser = std::make_shared<DB::User>(*UserTemplate_);
        newUser->name = userName;
        return newUser;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUsersManager> CreateUsersManager(
    TBootstrap* bootstrap,
    TString cliqueId)
{
    return std::make_unique<TUsersManager>(bootstrap, cliqueId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
