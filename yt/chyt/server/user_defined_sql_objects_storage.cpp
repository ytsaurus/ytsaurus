#include "user_defined_sql_objects_storage.h"

#include "config.h"
#include "host.h"
#include "private.h"

#include <yt/chyt/server/protos/clickhouse_service.pb.h>

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
using namespace NHydra;
using namespace NRe2;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSqlObjectInfo* protoInfo, const TSqlObjectInfo& info)
{
    protoInfo->set_revision(info.Revision);
    protoInfo->set_create_object_query(info.CreateObjectQuery);
}

void FromProto(TSqlObjectInfo* info, const NProto::TSqlObjectInfo& protoInfo)
{
    info->Revision = protoInfo.revision();
    info->CreateObjectQuery = protoInfo.create_object_query();
}

////////////////////////////////////////////////////////////////////////////////

class TUserDefinedSqlObjectsYTStorage
    : public DB::UserDefinedSQLObjectsStorageBase
    , public IUserDefinedSqlObjectsYTStorage
{
public:
    TUserDefinedSqlObjectsYTStorage(
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
            BIND(&TUserDefinedSqlObjectsYTStorage::SyncObjectsNonThrowing, this),
            Config_->UpdatePeriod))
    { }

    // DB::IUserDefinedSQLObjectsStorage overrides:

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

    // IUserDefinedSqlObjectsYTStorage overrides:

    bool TrySetObject(const String& objectName, const TSqlObjectInfo& info) override
    {
        YT_LOG_DEBUG("Setting SQL object (ObjectName: %v, Revision: %v)",
            objectName,
            info.Revision);

        auto lock = getLock();

        if (GetLastSeenObjectRevision(objectName) >= info.Revision) {
            return false;
        }

        setObject(objectName, *ParseCreateObjectQuery(objectName, info.CreateObjectQuery));
        LastSeenObjectRevisions_[objectName] = info.Revision;

        return true;
    }

    bool TryRemoveObject(const String& objectName, TRevision revision) override
    {
        YT_LOG_DEBUG("Removing SQL object (ObjectName: %v, Revision: %v)",
            objectName,
            revision);

        auto lock = getLock();

        if (GetLastSeenObjectRevision(objectName) >= revision) {
            return false;
        }

        removeObject(objectName);
        LastSeenObjectRevisions_[objectName] = revision;

        return true;
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

    THashMap<TString, TRevision> LastSeenObjectRevisions_;
    TRevision LastSeenRootRevision_ = NullRevision;

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

        auto nodeId = WaitFor(Client_->CreateNode(path, EObjectType::Document, options))
            .ValueOrThrow();

        auto revisionPath = Format("#%v/@revision", nodeId);
        auto revisionOrError = WaitFor(Client_->GetNode(revisionPath));
        if (revisionOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return false;
        }

        TSqlObjectInfo info{
            .Revision = ConvertTo<TRevision>(revisionOrError.ValueOrThrow()),
            .CreateObjectQuery = createStatement,
        };

        bool updated = TrySetObject(objectName, info);
        Host_->SetSqlObjectOnOtherInstances(TString(objectName), info);

        return updated;
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

        auto nodes = GetObjectMapNodeWithAttributes({"revision"});
        auto revision = nodes->Attributes().Get<TRevision>("revision");
        auto objectNode = nodes->FindChild(TString(objectName));

        if (objectNode) {
            return false;
        }

        bool removed = TryRemoveObject(objectName, revision);
        Host_->RemoveSqlObjectOnOtherInstances(TString(objectName), revision);

        return removed;
    }

    IMapNodePtr GetObjectMapNodeWithAttributes(std::vector<TString> attributes) const
    {
        TGetNodeOptions options;
        options.Attributes = std::move(attributes);
        auto nodesYson = WaitFor(Client_->GetNode(Config_->Path, options))
            .ValueOrThrow();

        return ConvertTo<IMapNodePtr>(nodesYson);
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

        auto nodes = GetObjectMapNodeWithAttributes({"value", "revision"});
        auto rootRevision = nodes->Attributes().Get<TRevision>("revision");

        THashMap<TString, TSqlObjectInfo> newObjectInfos;

        for (const auto& [name, node] : nodes->GetChildren()) {
            auto createStatement = node->Attributes().Get<TString>("value");
            newObjectInfos[name] = TSqlObjectInfo{
                .Revision = node->Attributes().Get<TRevision>("revision"),
                .CreateObjectQuery = createStatement,
            };
        }

        UpdateObjectInfos(rootRevision, std::move(newObjectInfos));
    }

    void UpdateObjectInfos(TRevision rootRevision, THashMap<TString, TSqlObjectInfo> newObjectInfos)
    {
        auto lock = getLock();

        YT_LOG_DEBUG("Updating object information (Revision: %v)", rootRevision);

        // Ignore delayed syncs.
        if (rootRevision < LastSeenRootRevision_) {
            return;
        }

        std::vector<TString> toRemove;
        for (const auto& [objectName, objectRevision] : LastSeenObjectRevisions_) {
            if (!newObjectInfos.contains(objectName)) {
                toRemove.push_back(objectName);
            }
        }
        for (const auto& objectName : toRemove) {
            if (TryRemoveObject(objectName, rootRevision)) {
                LastSeenObjectRevisions_.erase(objectName);
            }
        }

        for (const auto& [objectName, info] : newObjectInfos) {
            TrySetObject(objectName, info);
        }

        LastSuccessfulSyncTime_ = TInstant::Now();
        LastSeenRootRevision_ = rootRevision;
    }

    TRevision GetLastSeenObjectRevision(const String& objectName)
    {
        auto lock = getLock();

        if (auto it = LastSeenObjectRevisions_.find(objectName); it != LastSeenObjectRevisions_.end()) {
            return it->second;
        }
        return LastSeenRootRevision_;
    }

    DB::ASTPtr ParseCreateObjectQuery(const String& objectName, const TString& createObjectQuery)
    {
        DB::ParserCreateFunctionQuery parser;
        DB::ASTPtr ast;
        try {
            ast = DB::parseQuery(
                parser,
                createObjectQuery.data(),
                createObjectQuery.data() + createObjectQuery.size(),
                "" /*description*/,
                0 /*maxQuerySize*/,
                GlobalContext_->getSettingsRef().max_parser_depth);
        } catch (const std::exception& ex) {
            auto errorStatement = Format("create function %v as () -> throwIf(true, 'Failed to parse user defined function %v: %v')", objectName, objectName, ex.what());
            ast = DB::parseQuery(
                parser,
                errorStatement.data(),
                errorStatement.data() + errorStatement.size(),
                "" /*description*/,
                0 /*maxQuerySize*/,
                GlobalContext_->getSettingsRef().max_parser_depth);
        }
        return ast;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUserDefinedSQLObjectsStorage> CreateUserDefinedSqlObjectsYTStorage(
    DB::ContextPtr globalContext,
    TUserDefinedSqlObjectsStorageConfigPtr config,
    THost* host)
{
    return std::make_unique<TUserDefinedSqlObjectsYTStorage>(
        std::move(globalContext),
        std::move(config),
        host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
