#include "security_manager.h"

#include "format_helpers.h"

#include <yt/server/clickhouse_server/native/clique_authorization_manager.h>

//#include <Poco/Net/IPAddress.h>
//#include <Poco/Util/AbstractConfiguration.h>

//#include <Common/Exception.h>
//#include <Interpreters/Users.h>
//#include <Interpreters/ISecurityManager.h>

#include <util/generic/maybe.h>
#include <util/system/rwlock.h>

//#include <common/logger_useful.h>

#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
    extern const int IP_ADDRESS_NOT_ALLOWED;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_USER;
}

}

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using DB::String;
using UserPtr = DB::ISecurityManager::UserPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

static Poco::Logger* GetLogger()
{
    static Poco::Logger& logger = Poco::Logger::get("SecurityManager");
    return &logger;
}

}

////////////////////////////////////////////////////////////////////////////////

class TUserRegistry
{
private:
    using TUsers = std::map<String, UserPtr>;
    TUsers Users;
    TMaybe<DB::User> UserTemplate;
    TRWMutex RWMutex;

public:
    UserPtr GetOrRegisterNewUser(const String& userName);

    void Reload(Poco::Util::AbstractConfiguration& config);

private:
    UserPtr CreateNewUserFromTemplate(const String& userName);
};

////////////////////////////////////////////////////////////////////////////////

UserPtr TUserRegistry::GetOrRegisterNewUser(const String& userName)
{
    // Fast path

    {
        TReadGuard readerLock(RWMutex);

        auto found = Users.find(userName);
        if (found != Users.end()) {
            return found->second;
        }
    }

    // Slow path: register new user

    {
        TWriteGuard writerLock(RWMutex);

        auto found = Users.find(userName);
        if (found != Users.end()) {
            return found->second;
        }

        CH_LOG_INFO(GetLogger(), "Register new user " << Quoted(userName) << " from user template");

        auto inserted = Users.emplace(userName, CreateNewUserFromTemplate(userName));
        return inserted.first->second;
    }
}

void TUserRegistry::Reload(Poco::Util::AbstractConfiguration& config)
{
    TWriteGuard writerLock(RWMutex);

    Users.clear();
    UserTemplate.Clear();

    if (config.has("user_template")) {
        UserTemplate = DB::User("template", "user_template", config);
    }

    Poco::Util::AbstractConfiguration::Keys userNames;
    config.keys("users", userNames);

    for (const auto& userName : userNames) {
        CH_LOG_DEBUG(GetLogger(), "Load user " << Quoted(userName));
        auto user = std::make_shared<const DB::User>(userName, "users." + userName, config);
        Users.emplace(userName, std::move(user));
    }
}

UserPtr TUserRegistry::CreateNewUserFromTemplate(const String& userName)
{
    if (!UserTemplate.Defined()) {
        throw DB::Exception(
            "Cannot automatically register new user: user template not provided",
            DB::ErrorCodes::UNKNOWN_USER);
    };

    auto newUser = std::make_shared<DB::User>(*UserTemplate);
    newUser->name = userName;
    return newUser;
}

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public DB::ISecurityManager
{
private:
    mutable TUserRegistry Users;
    std::string CliqueId;
    NNative::ICliqueAuthorizationManagerPtr CliqueAuthorizationManager_;

public:
    TSecurityManager(std::string cliqueId, NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager);

    void loadFromConfig(Poco::Util::AbstractConfiguration& config) override;

    UserPtr authorizeAndGetUser(
        const String& userName,
        const String& password,
        const Poco::Net::IPAddress& address) const override;

    UserPtr getUser(const String& userName) const override;

    bool hasAccessToDatabase(
        const String& userName,
        const String& databaseName) const override;

private:
    void Authorize(
        const UserPtr& user,
        const String& password,
        const Poco::Net::IPAddress& address) const;
};

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(std::string cliqueId, NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
    : CliqueId(std::move(cliqueId))
    , CliqueAuthorizationManager_(std::move(cliqueAuthorizationManager))
{ }

void TSecurityManager::loadFromConfig(Poco::Util::AbstractConfiguration& config)
{
    Users.Reload(config);
}

UserPtr TSecurityManager::authorizeAndGetUser(
    const String& userName,
    const String& password,
    const Poco::Net::IPAddress& address) const
{
    auto user = Users.GetOrRegisterNewUser(userName);
    Authorize(user, password, address);
    CH_LOG_DEBUG(GetLogger(), "User authorized: " << Quoted(userName) << " (address: " << address.toString() << ")");
    return user;
}

UserPtr TSecurityManager::getUser(const String& userName) const
{
    return Users.GetOrRegisterNewUser(userName);
}

bool TSecurityManager::hasAccessToDatabase(
    const String& userName,
    const String& databaseName) const
{
    // At this point we only check if the user has access to current clique.
    // Storage layer is responsible for access control for specific tables.
    return CliqueAuthorizationManager_->HasAccess(userName);
}

void TSecurityManager::Authorize(
    const UserPtr& user,
    const String& password,
    const Poco::Net::IPAddress& address) const
{
    // Password intentionally ignored
    // User has already been authenticated in proxy
    Y_UNUSED(password);

    if (!user->addresses.contains(address)) {
        throw DB::Exception(
            "User " + Quoted(user->name) + " is not allowed to connect from address " + address.toString(),
            DB::ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::ISecurityManager> CreateSecurityManager(
    std::string cliqueId,
    NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager)
{
    return std::make_unique<TSecurityManager>(std::move(cliqueId), std::move(cliqueAuthorizationManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
