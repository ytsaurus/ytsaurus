#include "user_defined_sql_objects_storage.h"

#include "clickhouse_config.h"
#include "host.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/re2/re2.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>

#include <magic_enum.hpp>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NRe2;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

class UserDefinedSqlObjectsYTStorage
    : public DB::UserDefinedSQLObjectsStorageBase
{
public:
    UserDefinedSqlObjectsYTStorage(
        DB::ContextPtr globalContext,
        TUserDefinedSqlObjectsStorageConfigPtr config,
        THost* host)
        : GlobalContext_(std::move(globalContext))
        , Config_(std::move(config))
        , Host_(host)
        , Client_(Host_->CreateClient(ChytSqlObjectsUserName))
        , ObjectNameRegexp_(New<TRe2>("^[a-zA-Z_][a-zA-Z0-9_]*$"))
        , ActionQueue_(New<TActionQueue>("UserDefinedSqlObjectsStorage"))
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            // It's ok to use here 'this' instead of MakeWeak(this),
            // because executor's lifetime is less than UserDefinedSqlObjectsYTStorage's one.
            BIND(&UserDefinedSqlObjectsYTStorage::SyncObjectsNonThrowing, this),
            Config_->UpdatePeriod))
    { }

    bool isReplicated() const override
    {
        return true;
    }

    void loadObjects() override
    {
        if (!ObjectsLoaded_.exchange(true)) {
            YT_LOG_DEBUG("Loading user-defined objects");

            PeriodicExecutor_->Start();

            auto future = PeriodicExecutor_->GetExecutedEvent();
            PeriodicExecutor_->ScheduleOutOfBand();
            WaitForFast(future).ThrowOnError();
        }
    }

    void stopWatching() override
    {
        YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());
    }

    void reloadObjects() override
    {
        THROW_ERROR_EXCEPTION(
            "Method reloadObjects should not be called; this is a bug");
    }

    void reloadObject(DB::UserDefinedSQLObjectType /*objectType*/, const String& /*objectName*/) override
    {
        THROW_ERROR_EXCEPTION(
            "Method reloadObject should not be called; this is a bug");
    }

private:
    const DB::ContextPtr GlobalContext_;
    const TUserDefinedSqlObjectsStorageConfigPtr Config_;
    const THost* const Host_;
    const NNative::IClientPtr Client_;
    const TRe2Ptr ObjectNameRegexp_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    std::atomic<bool> ObjectsLoaded_ = false;
    TInstant LastSuccessfulSyncTime_ = TInstant::Now();

    bool storeObjectImpl(
        const DB::ContextPtr& currentContext,
        DB::UserDefinedSQLObjectType objectType,
        const String& objectName,
        DB::ASTPtr createObjectQuery,
        bool throwIfExists,
        bool replaceIfExists,
        const DB::Settings& /*settings*/) override
    {
        Host_->ValidateCliquePermission(TString(currentContext->getClientInfo().initial_user), EPermission::Manage);

        auto path = GetCypressPath(objectType, objectName);

        YT_LOG_DEBUG("Storing user-defined object (Path: %v)", path);

        auto createStatement = TString(DB::serializeAST(*createObjectQuery));

        TCreateNodeOptions options;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("value", ConvertToYsonString(createStatement));
        options.Force = !throwIfExists && replaceIfExists;
        options.IgnoreExisting = !throwIfExists && !replaceIfExists;

        WaitFor(Client_->CreateNode(path, EObjectType::Document, options))
            .ThrowOnError();

        return true;
    }

    bool removeObjectImpl(
        const DB::ContextPtr& currentContext,
        DB::UserDefinedSQLObjectType objectType,
        const String& objectName,
        bool throwIfNotExists) override
    {
        Host_->ValidateCliquePermission(TString(currentContext->getClientInfo().initial_user), EPermission::Manage);

        auto path = GetCypressPath(objectType, objectName);

        YT_LOG_DEBUG("Removing user-defined object (Path: %v)", path);

        TRemoveNodeOptions options;
        options.Force = !throwIfNotExists;

        WaitFor(Client_->RemoveNode(path, options))
            .ThrowOnError();

        return true;
    }

    TYPath GetCypressPath(DB::UserDefinedSQLObjectType objectType, const String& objectName) const
    {
        ValidateObjectName(objectName);

        switch (objectType) {
            case DB::UserDefinedSQLObjectType::Function:
                return TYPath(Format("%v/%v", Config_->Path, objectName));;
            default:
                THROW_ERROR_EXCEPTION("%v SQL object type is unsupported", magic_enum::enum_name(objectType));
        }
    }

    void ValidateObjectName(const String& objectName) const
    {
        if (!TRe2::FullMatch(objectName, *ObjectNameRegexp_)) {
            THROW_ERROR_EXCEPTION(
                "Object name %Qv is banned by regular expression %Qv",
                objectName,
                ObjectNameRegexp_->pattern());
        }
    }

    void SyncObjectsNonThrowing()
    {
        try {
            SyncObjects();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to sync objects");

            if (TInstant::Now() - LastSuccessfulSyncTime_ > Config_->ExpireAfterSuccessfulSyncTime) {
                auto lock = getLock();
                removeAllObjectsExcept({});
            }
        }
    }

    void SyncObjects()
    {
        YT_LOG_DEBUG("Synchronizing user-defined objects");

        TGetNodeOptions options;
        options.Attributes = {"value"};
        auto nodesYson = WaitFor(Client_->GetNode(Config_->Path, options))
            .ValueOrThrow();

        auto nodes = ConvertTo<IMapNodePtr>(nodesYson);

        std::vector<std::pair<String, DB::ASTPtr>> newFunctions;
        newFunctions.reserve(nodes->GetChildren().size());

        for (const auto& [name, node] : nodes->GetChildren()) {
            auto createStatement = node->Attributes().Get<TString>("value");

            DB::ParserCreateFunctionQuery parser;
            DB::ASTPtr ast;
            try {
                ast = DB::parseQuery(
                    parser,
                    createStatement.data(),
                    createStatement.data() + createStatement.size(),
                    "" /*description*/,
                    0 /*maxQuerySize*/,
                    GlobalContext_->getSettingsRef().max_parser_depth);
            } catch (const std::exception& ex) {
                auto errorStatement = Format("create function %v as () -> throwIf(true, 'Failed to parse user defined function %v: %v')", name, name, ex.what());
                ast = DB::parseQuery(
                    parser,
                    errorStatement.data(),
                    errorStatement.data() + errorStatement.size(),
                    "" /*description*/,
                    0 /*maxQuerySize*/,
                    GlobalContext_->getSettingsRef().max_parser_depth);
            }

            newFunctions.emplace_back(name, ast);
        }

        auto lock = getLock();
        setAllObjects(newFunctions);

        LastSuccessfulSyncTime_ = TInstant::Now();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUserDefinedSQLObjectsStorage> CreateUserDefinedSqlObjectsYTStorage(
    DB::ContextPtr globalContext,
    TUserDefinedSqlObjectsStorageConfigPtr config,
    THost* host)
{
    return std::make_unique<UserDefinedSqlObjectsYTStorage>(
        std::move(globalContext),
        std::move(config),
        host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
