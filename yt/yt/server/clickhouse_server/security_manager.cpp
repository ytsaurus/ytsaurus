#include "security_manager.h"

#include "config.h"

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/client/security_client/acl.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/ytree/permission.h>

#include <Interpreters/Users.h>
#include <Interpreters/IUsersManager.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/IPAddress.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NSecurityClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TLogger Logger("SecurityManager");

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public DB::IUsersManager
{
public:
    TSecurityManager(TSecurityManagerConfigPtr config, NApi::NNative::IClientPtr client, TGuid cliqueId)
        : Impl_(New<TImpl>(std::move(config), std::move(client), cliqueId))
    { }

    void loadFromConfig(const Poco::Util::AbstractConfiguration& config) override
    {
        WaitFor(BIND(&TImpl::LoadFromConfig, Impl_, ConstRef(config))
            .AsyncVia(Impl_->GetInvoker())
            .Run())
            .ThrowOnError();
    }

    UserPtr authorizeAndGetUser(
        const std::string& userName,
        const std::string& password,
        const Poco::Net::IPAddress& address) const override
    {
        return WaitFor(BIND(&TImpl::AuthorizeAndGetUser, Impl_)
            .AsyncVia(Impl_->GetInvoker())
            .Run(userName, password, address))
            .ValueOrThrow();
    }

    UserPtr getUser(const std::string& userName) const override
    {
        return WaitFor(BIND(&TImpl::GetUser, Impl_)
            .AsyncVia(Impl_->GetInvoker())
            .Run(userName))
            .ValueOrThrow();
    }

    bool hasAccessToDatabase(const std::string& userName, const std::string& databaseName) const override
    {
        return WaitFor(BIND(&TImpl::HasAccessToDatabase, Impl_)
            .AsyncVia(Impl_->GetInvoker())
            .Run(userName, databaseName))
            .ValueOrThrow();
    }

private:
    //! A single-threaded implementation of CH users (duh!) manager.
    //! Fetches current operation ACL in background.
    class TImpl
        : public TRefCounted
    {
    public:
        TImpl(TSecurityManagerConfigPtr config, NApi::NNative::IClientPtr client, TGuid cliqueId)
            : ActionQueue_(New<TActionQueue>("SecurityManager"))
            , Invoker_(ActionQueue_->GetInvoker())
            , Config_(std::move(config))
            , Client_(std::move(client))
            , UpdateExecutor_(New<TPeriodicExecutor>(
                Invoker_,
                BIND(&TImpl::DoUpdateCurrentAcl, MakeWeak(this)),
                Config_->OperationAclUpdatePeriod))
            , CliqueId_(std::move(cliqueId))
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (Config_->Enable) {
                YT_LOG_INFO("Updating ACL for the first time");
                UpdateExecutor_->Start();
            } else {
                YT_LOG_INFO("Security manager is disabled");
            }
        }

        void LoadFromConfig(const Poco::Util::AbstractConfiguration& config)
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            YT_LOG_DEBUG("Loading from config");

            Users_.clear();
            UserTemplate_ = std::nullopt;

            if (config.has("user_template")) {
                UserTemplate_ = DB::User("template", "user_template", config);
            }
        }

        UserPtr AuthorizeAndGetUser(
            const std::string& userName,
            const std::string& /* password */,
            const Poco::Net::IPAddress& /* address */)
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            YT_LOG_DEBUG("Authorizing and getting user (UserName: %v)", userName);

            return GetOrRegisterUser(userName);
        }

        UserPtr GetUser(const std::string& userName)
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            YT_LOG_DEBUG("Getting user (UserName: %v)", userName);

            return GetOrRegisterUser(userName);
        }

        bool HasAccessToDatabase(const std::string& userName, const std::string& databaseName)
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            YT_LOG_DEBUG("Checking user access to database (UserName: %v, DatabaseName: %v)", userName, databaseName);

            try {
                GetOrRegisterUser(TString(userName));
                return true;
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error while checking user access to database (UserName: %v, DatabaseName: %v)", userName, databaseName);
                return false;
            }
        }

        IInvokerPtr GetInvoker() const
        {
            VERIFY_THREAD_AFFINITY_ANY();

            return Invoker_;
        }

    private:
        TActionQueuePtr ActionQueue_;
        IInvokerPtr Invoker_;
        TSecurityManagerConfigPtr Config_;
        NApi::NNative::IClientPtr Client_;
        TPeriodicExecutorPtr UpdateExecutor_;

        THashMap<TString, DB::IUsersManager::UserPtr> Users_;
        std::optional<DB::User> UserTemplate_;

        TGuid CliqueId_;

        std::optional<TSerializableAccessControlList> CurrentAcl_;

        DB::IUsersManager::UserPtr GetOrRegisterUser(TStringBuf userName)
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            auto found = Users_.find(userName);
            if (found != Users_.end()) {
                YT_LOG_DEBUG("User found (UserName: %v)", userName);
                return found->second;
            }

            YT_LOG_INFO("Registering new user (UserName: %v)", userName);
            ValidateUserAccess(TString(userName));

            auto user = CreateNewUserFromTemplate(userName);
            auto result = Users_.emplace(userName, user);
            YT_VERIFY(result.second);
            return user;
        }

        void ValidateUserAccess(const TString& userName)
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            // At this point we only check if the user has access to current clique.
            // Storage layer is responsible for access control for specific tables.
            if (!Config_->Enable) {
                YT_LOG_DEBUG("Security manager is disabled, allowing access to user (User: %v)", userName);
                return;
            }

            if (userName == "default") {
                YT_LOG_DEBUG("Username is default, allowing this user to register");
                return;
            }

            if (userName == Client_->GetOptions().PinnedUser) {
                YT_LOG_DEBUG("Username is %Qv which is default user from config, allowing this user to register",
                    userName);
                return;
            }

            if (!CurrentAcl_) {
                YT_LOG_DEBUG("No operation ACL available, allowing access to user (User: %v)", userName);
                return;
            }

            NScheduler::ValidateOperationAccess(
                userName,
                CliqueId_,
                TGuid() /* jobId */,
                EPermission::Read,
                *CurrentAcl_,
                Client_,
                Logger);
            YT_LOG_DEBUG("User access validated using master, allowing access to user (User: %v)", userName);
        }

        void DoUpdateCurrentAcl()
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            try {
                auto newCurrentAcl = FetchCurrentAcl();

                YT_LOG_DEBUG("Current operation ACL fetched (Acl: %v)", ConvertToYsonString(newCurrentAcl, EYsonFormat::Text));
                if (CurrentAcl_ != newCurrentAcl) {
                    YT_LOG_INFO("Operation ACL has changed (OldAcl: %v, NewAcl: %v)",
                        ConvertToYsonString(CurrentAcl_, EYsonFormat::Text),
                        ConvertToYsonString(newCurrentAcl, EYsonFormat::Text));
                    Users_.clear();
                    CurrentAcl_ = newCurrentAcl;
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error updating operation ACL");
            }
        }

        std::optional<TSerializableAccessControlList> FetchCurrentAcl()
        {
            VERIFY_INVOKER_AFFINITY(Invoker_);

            NApi::TGetOperationOptions options;
            options.IncludeRuntime = true;
            options.Attributes = THashSet<TString>{"runtime_parameters"};
            auto runtimeParametersYsonString = WaitFor(Client_->GetOperation(
                CliqueId_,
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
            VERIFY_INVOKER_AFFINITY(Invoker_);

            if (!UserTemplate_) {
                THROW_ERROR_EXCEPTION("Cannot automatically register new user: user template not provided");
            };

            auto newUser = std::make_shared<DB::User>(*UserTemplate_);
            newUser->name = userName;
            return newUser;
        }
    };

    TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUsersManager> CreateSecurityManager(
    TSecurityManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    TGuid cliqueId)
{
    return std::make_unique<TSecurityManager>(std::move(config), std::move(client), cliqueId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
