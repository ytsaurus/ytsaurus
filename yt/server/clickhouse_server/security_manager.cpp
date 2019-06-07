#include "security_manager.h"

#include "format_helpers.h"
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

#include <common/logger_useful.h>

#include <map>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NSecurityClient;

static const auto& Logger = ServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TUsersManager
    : public DB::IUsersManager
{
private:
    THashMap<TString, DB::IUsersManager::UserPtr> Users_;
    std::optional<DB::User> UserTemplate_;
    TReaderWriterSpinLock SpinLock_;

    TBootstrap* Bootstrap_;
    TString CliqueId_;

    std::optional<TSerializableAccessControlList> CurrentAcl_;
    NProfiling::TCpuInstant LastCurrentAclUpdateTime_;

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

    bool hasAccessToDatabase(const std::string& userName, const std::string& /* databaseName */) const override
    {
        // At this point we only check if the user has access to current clique.
        // Storage layer is responsible for access control for specific tables.
        if (!Bootstrap_->GetConfig()->ValidateOperationAccess) {
            return true;
        }

        try {
            if (auto acl = const_cast<TUsersManager*>(this)->GetCurrentAcl()) {
                NScheduler::ValidateOperationAccess(
                    TString(userName),
                    TGuid::FromString(CliqueId_),
                    TGuid() /* jobId */,
                    EPermission::Read,
                    *acl,
                    Bootstrap_->GetRootClient(),
                    Logger);
            }
            return true;
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
                YT_LOG_INFO(ex, "User does not have access to the containing operation (User: %v, OperationId: %v)",
                    userName,
                    CliqueId_);
                return false;
            }
            throw;
        }
    }

private:
    DB::IUsersManager::UserPtr GetOrRegisterUser(TStringBuf userName)
    {
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

            auto user = CreateNewUserFromTemplate(userName);
            auto result = Users_.emplace(userName, user);
            YCHECK(result.second);
            return user;
        }
    }

    std::optional<TSerializableAccessControlList> GetCurrentAcl()
    {
        auto currentTime = NProfiling::GetCpuInstant();
        auto updatePeriod = NProfiling::DurationToCpuDuration(Bootstrap_->GetConfig()->OperationAclUpdatePeriod);
        {
            auto guard = TReaderGuard(SpinLock_);
            if (LastCurrentAclUpdateTime_ + updatePeriod >= currentTime) {
                return CurrentAcl_;
            }
        }
        {
            auto guard = TWriterGuard(SpinLock_);
            if (LastCurrentAclUpdateTime_ + updatePeriod < currentTime) {
                UpdateCurrentAcl();
            }
            return CurrentAcl_;
        }
    }

    void UpdateCurrentAcl()
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
            return;
        }
        auto aclNode = runtimeParametersNode
            ->AsMap()
            ->FindChild("acl");
        if (!aclNode) {
            return;
        }
        CurrentAcl_ = ConvertTo<TSerializableAccessControlList>(std::move(aclNode));
        LastCurrentAclUpdateTime_ = NProfiling::GetCpuInstant();
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
