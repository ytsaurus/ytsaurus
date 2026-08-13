#include "storage_yt_materialized_view.h"

#include "config.h"
#include "query_context.h"
#include "storage_distributor.h"
#include "table.h"
#include "yt_database_base.h"

#include <yt/yt/core/ypath/public.h>

#include <Databases/DatabaseOnDisk.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/getHeaderForProcessingStage.h>

#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Storages/SelectQueryDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageSnapshot.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStorageYtMaterializedView
    : public DB::IStorage
    , public IStorageYtMaterializedView
{
public:
    TStorageYtMaterializedView(
        const DB::StorageID& storageId,
        DB::ContextPtr localContext,
        const DB::ASTCreateQuery& createQuery,
        const DB::ColumnsDescription& columns,
        DB::StorageID targetTableId)
        : DB::IStorage(storageId)
        , TargetTableId_(std::move(targetTableId))
    {
        DB::StorageInMemoryMetadata storageMetadata;
        storageMetadata.setColumns(columns);
        // getSelectQueryFromASTForMatView may modify the passed context.
        storageMetadata.setSelectQuery(DB::SelectQueryDescription::getSelectQueryFromASTForMatView(
            createQuery.select->clone(),
            /*refreshable*/ false,
            DB::Context::createCopy(localContext)));
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

TMaterializedViewConfiguration BuildMaterializedViewConfiguration(
    const DB::ContextPtr& context,
    const DB::StoragePtr& table,
    const DB::ASTPtr& query)
{
    auto* queryContext = GetQueryContext(context);

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

    auto materializedView = dynamic_pointer_cast<DB::StorageMaterializedView>(table);
    YT_VERIFY(materializedView);

    auto resolveYtPath = [] (const DB::StorageID& storageId, TStringBuf role) {
        auto database = DB::DatabaseCatalog::instance().getDatabase(storageId.database_name);
        if (!dynamic_cast<TYtDatabaseBase*>(database.get())) {
            THROW_ERROR_EXCEPTION("Materialized view %v table %Qv must reside in a YT database",
                role,
                storageId.getFullTableName());
        }
        return TYPath(database->getTableDataPath(storageId.table_name));
    };

    auto selectTableId = materializedView->getInMemoryMetadataPtr()->getSelectQuery().select_table_id;
    if (selectTableId.empty()) {
        THROW_ERROR_EXCEPTION("Materialized view SELECT must read from a single YT table");
    }
    auto sourcePath = resolveYtPath(selectTableId, "source");
    auto targetPath = resolveYtPath(materializedView->getTargetTableId(), "target");

    std::vector<TTablePtr> sourceAndTarget;
    try {
        sourceAndTarget = FetchTablesSoft(
            queryContext,
            {TRichYPath::Parse(sourcePath), TRichYPath::Parse(targetPath)},
            /*skipUnsuitableNodes*/ false,
            /*enableDynamicStoreRead*/ true,
            queryContext->Logger);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Materialized view target table must exist and source table must be readable")
            .With(ex);
    }

    YT_VERIFY(sourceAndTarget.size() == 2);
    const auto& sourceTable = sourceAndTarget[0];
    const auto& targetTable = sourceAndTarget[1];
    if (targetTable->Dynamic) {
        THROW_ERROR_EXCEPTION("Materialized view target table must be static")
            .With("target_path", targetPath);
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
        .SourcePath = sourcePath,
        .TargetPath = targetPath,
        .SourceObjectId = sourceTable->ObjectId,
        .TargetObjectId = targetTable->ObjectId,
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
        context,
        createQuery,
        columns,
        DB::StorageID("YT", std::move(targetPath)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
