#include "storage_yt_materialized_view.h"

#include "config.h"
#include "query_context.h"
#include "storage_distributor.h"
#include "table.h"
#include "yt_database_base.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <Databases/DatabaseOnDisk.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getHeaderForProcessingStage.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Storages/SelectQueryDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageSnapshot.h>

#include <optional>
#include <utility>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Remove columns from targetHeader that do not exist in sourceHeader.
void RemoveNonCommonColumns(const DB::Block& sourceHeader, DB::Block& targetHeader)
{
    std::set<size_t> targetOnlyPositions;
    for (const auto& column : targetHeader) {
        if (!sourceHeader.has(column.name)) {
            targetOnlyPositions.insert(targetHeader.getPositionByName(column.name));
        }
    }
    targetHeader.erase(targetOnlyPositions);
}

const DB::ASTSelectQuery& GetSingleSelectQuery(const DB::ASTCreateQuery& create)
{
    const auto* selectWithUnion = create.select
        ? create.select->as<const DB::ASTSelectWithUnionQuery>()
        : nullptr;
    if (!selectWithUnion ||
        !selectWithUnion->list_of_selects ||
        selectWithUnion->list_of_selects->children.size() != 1)
    {
        THROW_ERROR_EXCEPTION("Materialized view SELECT must contain a single query");
    }

    const auto* select = selectWithUnion->list_of_selects->children[0]->as<const DB::ASTSelectQuery>();
    if (!select) {
        THROW_ERROR_EXCEPTION("Materialized view SELECT must contain a simple SELECT query");
    }

    return *select;
}

struct TMaterializedViewSource
{
    EMaterializedViewSourceType Type;
    std::optional<DB::StorageID> TableId;
    TYPath TableRangePath;
};

TMaterializedViewSource GetMaterializedViewSource(
    const DB::ASTCreateQuery& create,
    const DB::ContextPtr& context)
{
    const auto* tableExpression = GetSingleTableExpression(&GetSingleSelectQuery(create));
    if (!tableExpression) {
        THROW_ERROR_EXCEPTION("Materialized view SELECT has malformed table expression");
    }

    if (tableExpression->table_function) {
        const auto* function = tableExpression->table_function->as<const DB::ASTFunction>();
        if (!function || function->name != "concatYtTablesRange") {
            THROW_ERROR_EXCEPTION(
                "Materialized view SELECT must read from a single YT table or concatYtTablesRange");
        }

        const auto& arguments = function->arguments->as<const DB::ASTExpressionList&>().children;
        if (arguments.size() != 1) {
            THROW_ERROR_EXCEPTION(
                "Materialized view concatYtTablesRange source requires exactly one directory argument");
        }

        auto argument = arguments[0]->clone();
        argument = DB::evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
        const auto* literal = argument->as<const DB::ASTLiteral>();
        if (!literal || literal->value.getType() != DB::Field::Types::String) {
            THROW_ERROR_EXCEPTION("Materialized view concatYtTablesRange directory must be a constant string");
        }

        return {
            .Type = EMaterializedViewSourceType::TableRange,
            .TableRangePath = TRichYPath::Parse(literal->value.safeGet<std::string>()).GetPath(),
        };
    }

    const auto* identifier = tableExpression->database_and_table_name
        ? tableExpression->database_and_table_name->as<const DB::ASTTableIdentifier>()
        : nullptr;
    if (!identifier) {
        THROW_ERROR_EXCEPTION(
            "Materialized view SELECT must read from a single YT table or concatYtTablesRange");
    }

    auto tableId = identifier->getTableId();
    if (tableId.database_name.empty()) {
        tableId.database_name = context->getCurrentDatabase();
    }
    return {
        .Type = EMaterializedViewSourceType::StaticTable,
        .TableId = std::move(tableId),
    };
}

TObjectId GetTableRangeObjectId(TQueryContext* queryContext, const TYPath& path)
{
    TListNodeOptions listOptions;
    static_cast<TMasterReadOptions&>(listOptions) = *queryContext->SessionSettings->CypressReadOptions;
    listOptions.Attributes = {"path", "type", "dynamic"};
    auto children = ConvertTo<IListNodePtr>(WaitFor(queryContext->Client()->ListNode(path, listOptions))
        .ValueOrThrow())
        ->GetChildren();

    for (const auto& child : children) {
        const auto& attributes = child->Attributes();
        if (attributes.Get<EObjectType>("type") != EObjectType::Table) {
            continue;
        }
        THROW_ERROR_EXCEPTION_IF(attributes.Find<bool>("dynamic").value_or(false),
            "Materialized view concatYtTablesRange source contains a dynamic table")
            .With("source_path", attributes.Get<TYPath>("path"));
    }

    TGetNodeOptions getOptions;
    static_cast<TMasterReadOptions&>(getOptions) = *queryContext->SessionSettings->CypressReadOptions;
    return ConvertTo<TObjectId>(WaitFor(queryContext->Client()->GetNode(path + "/@id", getOptions))
        .ValueOrThrow());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

const DB::ASTTableExpression* GetSingleTableExpression(const DB::ASTSelectQuery* selectQuery)
{
    if (!selectQuery) {
        return nullptr;
    }

    const auto& tables = selectQuery->tables();
    if (!tables || tables->children.size() != 1) {
        THROW_ERROR_EXCEPTION("Materialized view SELECT must read from a single source");
    }

    const auto* element = tables->children[0]->as<const DB::ASTTablesInSelectQueryElement>();
    if (!element || !element->table_expression) {
        return nullptr;
    }

    return element->table_expression->as<const DB::ASTTableExpression>();
}

DB::ASTTableExpression* GetSingleTableExpression(DB::ASTSelectQuery* selectQuery)
{
    return const_cast<DB::ASTTableExpression*>(
        GetSingleTableExpression(static_cast<const DB::ASTSelectQuery*>(selectQuery)));
}

////////////////////////////////////////////////////////////////////////////////

class TStorageYtMaterializedView
    : public DB::IStorage
    , public IStorageYtMaterializedView
{
public:
    TStorageYtMaterializedView(
        const DB::StorageID& storageId,
        const DB::ASTCreateQuery& createQuery,
        const DB::ColumnsDescription& columns,
        DB::StorageID targetTableId,
        const std::string& comment = {})
        : DB::IStorage(storageId)
        , TargetTableId_(std::move(targetTableId))
    {
        DB::StorageInMemoryMetadata storageMetadata;
        storageMetadata.setColumns(columns);
        if (!createQuery.select) {
            THROW_ERROR_EXCEPTION("Materialized view SELECT query is not specified");
        }

        DB::SelectQueryDescription selectQueryDescription;
        selectQueryDescription.select_query = createQuery.select->clone();
        selectQueryDescription.inner_query = GetSingleSelectQuery(createQuery).clone();
        storageMetadata.setSelectQuery(std::move(selectQueryDescription));
        if (createQuery.sql_security) {
            storageMetadata.setSQLSecurity(createQuery.sql_security->as<DB::ASTSQLSecurity&>());
        }
        if (!comment.empty()) {
            storageMetadata.setComment(comment);
        }
        setInMemoryMetadata(storageMetadata);
    }

    std::string getName() const override { return "MaterializedView"; }

    bool isView() const override { return true; }

    IStorageDistributorPtr ResolveTargetDistributor(DB::ContextPtr context) const override
    {
        return std::dynamic_pointer_cast<IStorageDistributor>(ResolveTarget(context));
    }

    void read(
        DB::QueryPlan& queryPlan,
        const DB::Names& columnNames,
        const DB::StorageSnapshotPtr& storageSnapshot,
        DB::SelectQueryInfo& queryInfo,
        DB::ContextPtr localContext,
        DB::QueryProcessingStage::Enum processedStage,
        size_t maxBlockSize,
        size_t numStreams) override
    {
        auto storage = ResolveTarget(localContext);
        auto targetMetadataSnapshot = storage->getInMemoryMetadataPtr();
        auto targetStorageSnapshot = storage->getStorageSnapshot(targetMetadataSnapshot, localContext);

        storage->read(queryPlan, columnNames, targetStorageSnapshot, queryInfo, localContext, processedStage, maxBlockSize, numStreams);

        if (queryPlan.isInitialized()) {
            auto viewHeader = DB::getHeaderForProcessingStage(columnNames, storageSnapshot, queryInfo, localContext, processedStage);
            auto targetHeader = queryPlan.getCurrentHeader();

            RemoveNonCommonColumns(viewHeader, targetHeader);
            RemoveNonCommonColumns(targetHeader, viewHeader);

            if (!DB::blocksHaveEqualStructure(viewHeader, targetHeader)) {
                auto convertingActions = DB::ActionsDAG::makeConvertingActions(
                    targetHeader.getColumnsWithTypeAndName(),
                    viewHeader.getColumnsWithTypeAndName(),
                    DB::ActionsDAG::MatchColumnsMode::Name);
                convertingActions.removeUnusedActions();
                auto convertingStep = std::make_unique<DB::ExpressionStep>(queryPlan.getCurrentHeader(), std::move(convertingActions));
                convertingStep->setStepDescription("Convert target table structure to MaterializedView structure");
                queryPlan.addStep(std::move(convertingStep));
            }

            queryPlan.addStorageHolder(storage);
        }
    }

    DB::SinkToStoragePtr write(
        const DB::ASTPtr& /*query*/,
        const DB::StorageMetadataPtr& /*metadataSnapshot*/,
        DB::ContextPtr /*context*/,
        bool /*asyncInsert*/) override
    {
        THROW_ERROR_EXCEPTION("Write to a materialized view is not supported in CHYT");
    }

    DB::QueryProcessingStage::Enum getQueryProcessingStage(
        DB::ContextPtr localContext,
        DB::QueryProcessingStage::Enum toStage,
        const DB::StorageSnapshotPtr& /*storageSnapshot*/,
        DB::SelectQueryInfo& queryInfo) const override
    {
        auto target = ResolveTarget(localContext);
        return target->getQueryProcessingStage(
            localContext,
            toStage,
            target->getStorageSnapshot(target->getInMemoryMetadataPtr(), localContext),
            queryInfo);
    }

    DB::StorageSnapshotPtr getStorageSnapshot(
        const DB::StorageMetadataPtr& metadataSnapshot,
        DB::ContextPtr queryContext) const override
    {
        return std::make_shared<DB::StorageSnapshot>(*this, metadataSnapshot, ResolveTarget(queryContext)->getVirtualsPtr());
    }

    // The target is always a YT table backed by TStorageDistributor,
    // so these mirror its class constants instead of resolving the target.
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsParallelInsert() const override { return true; }
    bool isRemote() const override { return true; }

private:
    const DB::StorageID TargetTableId_;

    DB::StoragePtr ResolveTarget(DB::ContextPtr context) const
    {
        return DB::DatabaseCatalog::instance().getTable(TargetTableId_, context);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterStorageYtMaterializedView(DB::StorageFactory& factory)
{
    factory.registerStorage("MaterializedView", [] (const DB::StorageFactory::Arguments& args) -> DB::StoragePtr {
        auto database = DB::DatabaseCatalog::instance().getDatabase(args.table_id.database_name);
        if (dynamic_cast<TYtDatabaseBase*>(database.get())) {
            return std::make_shared<TStorageYtMaterializedView>(
                args.table_id,
                args.query,
                args.columns,
                args.query.getTargetTableID(DB::ViewTarget::To),
                args.comment);
        }

        return std::make_shared<DB::StorageMaterializedView>(
            args.table_id,
            args.getLocalContext(),
            args.query,
            args.columns,
            args.mode,
            args.comment,
            args.is_restore_from_backup);
    });
}

////////////////////////////////////////////////////////////////////////////////

TMaterializedViewConfiguration BuildMaterializedViewConfiguration(
    const DB::ContextPtr& context,
    const DB::StoragePtr& table,
    const DB::ASTPtr& query)
{
    auto* queryContext = GetQueryContext(context);
    auto materializedView = dynamic_pointer_cast<IStorageYtMaterializedView>(table);
    YT_VERIFY(materializedView);

    const auto& create = query->as<const DB::ASTCreateQuery&>();
    if (create.is_populate) {
        THROW_ERROR_EXCEPTION("POPULATE is not supported for materialized views in CHYT");
    }
    if (create.refresh_strategy) {
        THROW_ERROR_EXCEPTION("REFRESH is not supported for materialized views in CHYT");
    }
    if (create.getTargetTableID(DB::ViewTarget::To).empty()) {
        THROW_ERROR_EXCEPTION("Materialized views in CHYT require an explicit TO clause with a target YT table");
    }

    auto cloned = query->clone();
    auto& clonedCreate = cloned->as<DB::ASTCreateQuery&>();

    // Background view updates are executed on behalf of the creator, so the persisted
    // statement must pin a concrete definer; an omitted or CURRENT_USER clause falls
    // back to the query user.
    const auto& user = context->getClientInfo().initial_user;
    if (!clonedCreate.sql_security) {
        clonedCreate.sql_security = std::make_shared<DB::ASTSQLSecurity>();
    }
    auto& sqlSecurity = clonedCreate.sql_security->as<DB::ASTSQLSecurity&>();
    if (sqlSecurity.type && *sqlSecurity.type != SQLSecurityType::DEFINER) {
        THROW_ERROR_EXCEPTION("Materialized views in CHYT support only SQL SECURITY DEFINER");
    }
    if (!sqlSecurity.definer || sqlSecurity.is_definer_current_user) {
        sqlSecurity.type = SQLSecurityType::DEFINER;
        sqlSecurity.is_definer_current_user = false;
        sqlSecurity.definer = std::make_shared<DB::ASTUserNameWithHost>(user);
    } else if (auto definer = sqlSecurity.definer->toString(); definer != user) {
        THROW_ERROR_EXCEPTION("Materialized view definer %Qv must match the query user %Qv",
            definer,
            user);
    }

    auto source = GetMaterializedViewSource(clonedCreate, context);

    auto getSingleTable = [] (const IStorageDistributorPtr& distributor, TStringBuf role) {
        THROW_ERROR_EXCEPTION_IF(!distributor,
            "Materialized view %v table must be a YT table",
            role);

        auto tables = distributor->GetTables();
        THROW_ERROR_EXCEPTION_IF(tables.size() != 1,
            "Materialized view %v table must resolve to a single YT table",
            role);
        return tables[0];
    };

    TTablePtr sourceTable;
    TYPath sourcePath;
    if (source.Type == EMaterializedViewSourceType::StaticTable) {
        auto sourceStorage = DB::DatabaseCatalog::instance().getTable(*source.TableId, context);
        sourceTable = getSingleTable(
            std::dynamic_pointer_cast<IStorageDistributor>(sourceStorage),
            "source");
        sourcePath = sourceTable->GetPath();
    } else {
        sourcePath = source.TableRangePath;
    }

    auto targetTable = getSingleTable(
        materializedView->ResolveTargetDistributor(context),
        "target");
    auto targetPath = targetTable->GetPath();
    if (targetTable->Dynamic) {
        THROW_ERROR_EXCEPTION("Materialized view target table must be static")
            .With("target_path", targetPath);
    }

    if (source.Type == EMaterializedViewSourceType::TableRange) {
        auto sourceObjectId = GetTableRangeObjectId(queryContext, sourcePath);
        return {
            .CreateStatement = DB::getObjectDefinitionFromCreateQuery(cloned),
            .SourceType = source.Type,
            .SourcePath = sourcePath,
            .TargetPath = targetPath,
            .SourceObjectId = sourceObjectId,
        };
    }

    if (sourceTable->Dynamic) {
        THROW_ERROR_EXCEPTION("Materialized view source table must be static")
            .With("source_path", sourcePath);
    }

    THROW_ERROR_EXCEPTION_IF(!sourceTable->RowCount,
        "Materialized view source table has no row count")
        .With("source_path", sourcePath);

    return {
        .CreateStatement = DB::getObjectDefinitionFromCreateQuery(cloned),
        .SourceType = source.Type,
        .SourcePath = sourcePath,
        .TargetPath = targetPath,
        .SourceObjectId = sourceTable->ObjectId,
    };
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageYtMaterializedView(
    const DB::StorageID& storageId,
    const DB::ASTCreateQuery& createQuery,
    NYPath::TYPath targetPath,
    DB::ContextPtr context)
{
    auto columns = DB::InterpreterCreateQuery::getColumnsDescription(
        *createQuery.columns_list->columns,
        context,
        DB::LoadingStrictnessLevel::ATTACH);

    return std::make_shared<TStorageYtMaterializedView>(
        storageId,
        createQuery,
        columns,
        DB::StorageID("YT", std::move(targetPath)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
