#include "storage_subquery.h"

#include "config.h"
#include "secondary_query_source.h"
#include "read_plan.h"
#include "granule_min_max_filter.h"
#include "prewhere_secondary_query_source.h"
#include "query_context.h"
#include "storage_base.h"
#include "subquery_spec.h"

#include <yt/yt/ytlib/table_client/virtual_value_directory.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <QueryPipeline/Pipe.h>

namespace NYT::NClickHouseServer {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnSchema> GetColumnSchemas(
    const TSubquerySpec& subquerySpec,
    const DB::Names& columnNames)
{
    std::vector<TColumnSchema> result;
    result.reserve(columnNames.size());

    TTableSchemaPtr virtualValueSchema;
    if (!subquerySpec.DataSourceDirectory->DataSources().empty()) {
        auto virtualValueDirectory = subquerySpec.DataSourceDirectory->DataSources().front().GetVirtualValueDirectory();

        // Sanity check.
        for (const auto& dataSource : subquerySpec.DataSourceDirectory->DataSources()) {
            if (virtualValueDirectory) {
                YT_VERIFY(dataSource.GetVirtualValueDirectory());
                YT_VERIFY(*dataSource.GetVirtualValueDirectory()->Schema == *virtualValueDirectory->Schema);
            } else {
                YT_VERIFY(!dataSource.GetVirtualValueDirectory());
            }
        }

        if (virtualValueDirectory) {
            virtualValueSchema = virtualValueDirectory->Schema;
        }
    }

    for (const auto& columnName : columnNames) {
        if (const auto* column = subquerySpec.ReadSchema->FindColumn(columnName)) {
            result.push_back(*column);
        } else if (const auto* column = virtualValueSchema ? virtualValueSchema->FindColumn(columnName) : nullptr) {
            result.push_back(*column);
        } else {
            THROW_ERROR_EXCEPTION("No such column %Qv", columnName);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TStorageSubquery
    : public TYtStorageBase
{
public:
    TStorageSubquery(TQueryContext* queryContext, TSubquerySpec subquerySpec)
        : TYtStorageBase({"subquery_db", "subquery"})
        , QueryContext_(queryContext)
        , SubquerySpec_(std::move(subquerySpec))
    {
        if (QueryContext_->InitialQuery) {
            YT_VERIFY(*QueryContext_->InitialQuery == SubquerySpec_.InitialQuery);
        } else {
            QueryContext_->InitialQuery = SubquerySpec_.InitialQuery;
        }

        DB::StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(DB::ColumnsDescription(ToNamesAndTypesList(*SubquerySpec_.ReadSchema, SubquerySpec_.QuerySettings->Composite)));
        setInMemoryMetadata(storage_metadata);

        QueryContext_->MoveToPhase(EQueryPhase::Preparation);
    }

    std::string getName() const override
    {
        return "YT";
    }

    bool supportsPrewhere() const override
    {
        return true;
    }

    bool isRemote() const override
    {
        // NB: from CH point of view this is already a non-remote query.
        // If we return here, GLOBAL IN stops working: CHYT-117.
        return false;
    }

    DB::Pipe read(
        const DB::Names& columnNames,
        const DB::StorageSnapshotPtr& storageSnapshot,
        DB::SelectQueryInfo& queryInfo,
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum /*processedStage*/,
        size_t /*maxBlockSize*/,
        size_t maxStreamCount) override
    {
        NTracing::TChildTraceContextGuard traceContextGuard(
            QueryContext_->TraceContext,
            "ClickHouseYt.PreparePipes");

        using namespace NStatisticPath;
        auto timerGuard = QueryContext_->CreateStatisticsTimerGuard("/storage_subquery/read"_SP);

        QueryContext_->MoveToPhase(EQueryPhase::Execution);

        auto metadataSnapshot = storageSnapshot->getMetadataForQuery();
        auto [realColumnNames, virtualColumnNames] = DecoupleColumns(columnNames, metadataSnapshot);

        StorageContext_ = QueryContext_->GetOrRegisterStorageContext(this, context);

        if (StorageContext_->Settings->Testing->ThrowExceptionInSubquery) {
            THROW_ERROR_EXCEPTION("Testing exception in subquery")
                << TErrorAttribute("storage_index", StorageContext_->Index);
        }

        if (StorageContext_->Settings->Testing->SubqueryAllocationSize > 0) {
            // Make an intentional memory leak.
            auto* leakedMemory = new char[StorageContext_->Settings->Testing->SubqueryAllocationSize];
            for (i64 index = 0; index != StorageContext_->Settings->Testing->SubqueryAllocationSize; ++index) {
                leakedMemory[index] = (index < 2) ? 1 : leakedMemory[index - 2] + leakedMemory[index - 1];
            }
            // Prevent optimization.
            YT_LOG_DEBUG(
                "Testing Fibbonacci number calculated (Index: %v, Value: %v)",
                StorageContext_->Settings->Testing->SubqueryAllocationSize - 1,
                leakedMemory[StorageContext_->Settings->Testing->SubqueryAllocationSize - 1]);
        }

        Logger = StorageContext_->Logger;
        Logger.AddTag("SubqueryIndex: %v, SubqueryTableIndex: %v",
            SubquerySpec_.SubqueryIndex,
            SubquerySpec_.TableIndex);

        const auto& traceContext = GetQueryContext(context)->TraceContext;

        auto perThreadDataSliceDescriptors = SubquerySpec_.DataSliceDescriptors;

        i64 totalRowCount = 0;
        i64 totalDataWeight = 0;
        i64 totalDataSliceCount = 0;
        for (const auto& threadDataSliceDescriptors : perThreadDataSliceDescriptors) {
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                totalRowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
                totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                ++totalDataSliceCount;
            }
        }
        YT_LOG_DEBUG(
            "Deserialized subquery spec (RowCount: %v, DataWeight: %v, DataSliceCount: %v)",
            totalRowCount,
            totalDataWeight,
            totalDataSliceCount);


        auto tableIndexSuffix = Format(".%v", SubquerySpec_.TableIndex);
        traceContext->AddTag("chyt.row_count" + tableIndexSuffix, totalRowCount);
        traceContext->AddTag("chyt.data_weight" + tableIndexSuffix, totalDataWeight);
        traceContext->AddTag("chyt.data_slice_count" + tableIndexSuffix, totalDataSliceCount);
        if (SubquerySpec_.TableIndex == 0) {
            traceContext->AddTag("chyt.subquery_index", SubquerySpec_.SubqueryIndex);
        }

        YT_LOG_DEBUG(
            "Creating table readers (MaxStreamCount: %v, StripeCount: %v, Columns: %v)",
            maxStreamCount,
            SubquerySpec_.DataSliceDescriptors.size(),
            columnNames);

        YT_LOG_INFO("Creating table readers");
        DB::Pipes pipes;

        IGranuleFilterPtr granuleMinMaxFilter;
        if (StorageContext_->Settings->Execution->EnableMinMaxFiltering) {
            granuleMinMaxFilter = CreateGranuleMinMaxFilter(
                queryInfo,
                StorageContext_->Settings->Composite,
                SubquerySpec_.ReadSchema,
                context,
                realColumnNames);
        }

        auto statisticsCallback = BIND([weakQueryContext = MakeWeak(QueryContext_)] (const TStatistics& statistics) {
            if (auto queryContext = weakQueryContext.Lock()) {
                queryContext->MergeStatistics(statistics);
            }
        });

        auto columnSchemas = GetColumnSchemas(SubquerySpec_, columnNames);

        TReadPlanWithFilterPtr readPlan;
        if (queryInfo.prewhere_info) {
            readPlan = BuildReadPlanWithPrewhere(
                columnSchemas,
                queryInfo.prewhere_info,
                context->getSettingsRef());
        } else {
            readPlan = BuildSimpleReadPlan(columnSchemas);
        }

        for (int threadIndex = 0; threadIndex < std::ssize(perThreadDataSliceDescriptors); ++threadIndex) {
            const auto& threadDataSliceDescriptors = perThreadDataSliceDescriptors[threadIndex];

            DB::SourcePtr sourcePtr;
            if (StorageContext_->Settings->Prewhere->PrefilterDataSlices && readPlan->SuitableForTwoStagePrewhere()) {
                sourcePtr = CreatePrewhereSecondaryQuerySource(
                    StorageContext_,
                    SubquerySpec_,
                    readPlan,
                    traceContext,
                    threadDataSliceDescriptors,
                    granuleMinMaxFilter,
                    statisticsCallback);
            } else {
                sourcePtr = CreateSecondaryQuerySource(
                    StorageContext_,
                    SubquerySpec_,
                    readPlan,
                    traceContext,
                    threadDataSliceDescriptors,
                    granuleMinMaxFilter,
                    statisticsCallback);
            }
            pipes.emplace_back(sourcePtr);

            i64 rowCount = 0;
            i64 dataWeight = 0;
            int dataSliceCount = 0;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                    rowCount += chunkSpec.row_count_override();
                    dataWeight += chunkSpec.data_weight_override();
                }
                ++dataSliceCount;

                if (SubquerySpec_.DataSourceDirectory->DataSources().front().GetType() == EDataSourceType::UnversionedTable) {
                    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                        if (FromProto<TTabletId>(chunkSpec.tablet_id()) != NullTabletId) {
                            // In case of ordered dynamic tables, dynamic stores do not have misc ext.
                            // NB: This seems to be the easiest way to check without dragging additional info into TDataSource/TSubquerySpec.
                            continue;
                        }
                        // It is crucial for better memory estimation.
                        YT_VERIFY(FindProtoExtension<NChunkClient::NProto::TMiscExt>(
                            chunkSpec.chunk_meta().extensions()));
                    }
                }
            }
            sourcePtr->addTotalRowsApprox(rowCount);
            sourcePtr->addTotalBytes(dataWeight);
            YT_LOG_DEBUG(
                "Thread table reader stream created (ThreadIndex: %v, RowCount: %v, DataWeight: %v, DataSliceCount: %v)",
                threadIndex,
                rowCount,
                dataWeight,
                dataSliceCount);

            TStringBuilder debugString;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                debugString.AppendString(ToString(dataSliceDescriptor));
                debugString.AppendString("\n");
            }

            YT_LOG_DEBUG(
                "Thread debug string (ThreadIndex: %v, DebugString: %v)",
                threadIndex,
                debugString.Flush());
        }

        return DB::Pipe::unitePipes(std::move(pipes));
    }

    DB::QueryProcessingStage::Enum getQueryProcessingStage(
        DB::ContextPtr /*context*/,
        DB::QueryProcessingStage::Enum /*toStage*/,
        const DB::StorageSnapshotPtr&,
        DB::SelectQueryInfo& /*queryInfo*/) const override
    {
        return DB::QueryProcessingStage::Enum::FetchColumns;
    }

private:
    TStorageContext* StorageContext_ = nullptr;
    TQueryContext* QueryContext_;
    TSubquerySpec SubquerySpec_;
    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSubquery(TQueryContext* queryContext, TSubquerySpec subquerySpec)
{
    return std::make_shared<TStorageSubquery>(queryContext, std::move(subquerySpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
