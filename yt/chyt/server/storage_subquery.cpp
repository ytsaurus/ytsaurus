#include "storage_subquery.h"

#include "block_input_stream.h"
#include "config.h"
#include "prewhere_block_input_stream.h"
#include "query_context.h"
#include "subquery_spec.h"
#include "query_registry.h"
#include "storage_base.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <QueryPipeline/Pipe.h>

namespace NYT::NClickHouseServer {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;
using namespace NTracing;

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
        unsigned maxStreamCount) override
    {
        NTracing::TChildTraceContextGuard traceContextGuard(
            QueryContext_->TraceContext,
            "ClickHouseYt.PreparePipes");

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

        auto schema = SubquerySpec_.ReadSchema;

        YT_LOG_INFO("Creating table readers");
        DB::Pipes pipes;
        const auto& prewhereInfo = queryInfo.prewhere_info;

        if (SubquerySpec_.DataSourceDirectory->DataSources()[0].GetType() == EDataSourceType::VersionedTable && prewhereInfo) {
            // TODO(max42): CHYT-462.
            THROW_ERROR_EXCEPTION("PREWHERE is not supported for dynamic tables (CHYT-462)");
        }

        for (int threadIndex = 0;
            threadIndex < static_cast<int>(perThreadDataSliceDescriptors.size());
            ++threadIndex)
        {
            const auto& threadDataSliceDescriptors = perThreadDataSliceDescriptors[threadIndex];

            if (prewhereInfo) {
                pipes.emplace_back(std::make_shared<DB::SourceFromInputStream>(CreatePrewhereBlockInputStream(
                    StorageContext_,
                    SubquerySpec_,
                    realColumnNames,
                    virtualColumnNames,
                    traceContext,
                    threadDataSliceDescriptors,
                    prewhereInfo)));
            } else {
                pipes.emplace_back(std::make_shared<DB::SourceFromInputStream>(CreateBlockInputStream(
                    StorageContext_,
                    SubquerySpec_,
                    realColumnNames,
                    virtualColumnNames,
                    traceContext,
                    threadDataSliceDescriptors,
                    prewhereInfo)));
            }

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
                        // It is crucial for better memory estimation.
                        YT_VERIFY(FindProtoExtension<NChunkClient::NProto::TMiscExt>(
                            chunkSpec.chunk_meta().extensions()));
                    }
                }
            }
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
