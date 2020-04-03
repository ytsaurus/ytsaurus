#include "storage_subquery.h"

#include "block_input_stream.h"
#include "config.h"
#include "query_context.h"
#include "subquery_spec.h"
#include "query_registry.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

Names ExtractColumnsFromPrewhereInfo(PrewhereInfoPtr prewhereInfo)
{
    Names prewhereColumns;
    if (prewhereInfo->alias_actions) {
        prewhereColumns = prewhereInfo->alias_actions->getRequiredColumns();
    } else {
        prewhereColumns = prewhereInfo->prewhere_actions->getRequiredColumns();
    }
    return prewhereColumns;
}

TTableReaderConfigPtr CreateTableReaderConfig()
{
    auto config = New<TTableReaderConfig>();
    config->GroupSize = 150_MB;
    config->WindowSize = 200_MB;
    return config;
}

TClientBlockReadOptions CreateBlockReadOptions(const TString& user)
{
    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
    blockReadOptions.WorkloadDescriptor.CompressionFairShareTag = user;
    blockReadOptions.ReadSessionId = NChunkClient::TReadSessionId::Create();
    return blockReadOptions;
}

NTableClient::TTableSchema FilterColumnsInSchema(
    const NTableClient::TTableSchema& schema,
    Names columnNames)
{
    std::vector<TString> columnNamesInString;
    for (const auto& columnName : columnNames) {
        if (!schema.FindColumn(columnName)) {
            THROW_ERROR_EXCEPTION("Column not found")
                << TErrorAttribute("column", columnName);
        }
        columnNamesInString.emplace_back(columnName);
    }
    return schema.Filter(columnNamesInString);
}

std::pair<BlockInputStreamPtr, ISchemalessMultiChunkReaderPtr> CreateBlockInputStreamAndReader(
    TQueryContext* queryContext,
    const TSubquerySpec& subquerySpec,
    const Names& columnNames,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    PrewhereInfoPtr prewhereInfo)
{
    auto schema = FilterColumnsInSchema(subquerySpec.ReadSchema, columnNames);
    auto blockReadOptions = CreateBlockReadOptions(queryContext->User);

    auto blockInputStreamTraceContext = NTracing::CreateChildTraceContext(
        traceContext,
        "ClickHouseYt.BlockInputStream");
    NTracing::TTraceContextGuard guard(blockInputStreamTraceContext);

    auto readerMemoryManager = queryContext->Bootstrap->GetHost()->GetMultiReaderMemoryManager()->CreateMultiReaderMemoryManager(
        queryContext->Bootstrap->GetConfig()->ReaderMemoryRequirement,
        {queryContext->UserTagId});
    auto reader = CreateSchemalessParallelMultiReader(
        CreateTableReaderConfig(),
        New<NTableClient::TTableReaderOptions>(),
        queryContext->Client(),
        /* localDescriptor =*/{},
        std::nullopt,
        queryContext->Client()->GetNativeConnection()->GetBlockCache(),
        queryContext->Client()->GetNativeConnection()->GetNodeDirectory(),
        subquerySpec.DataSourceDirectory,
        dataSliceDescriptors,
        TNameTable::FromSchema(schema),
        blockReadOptions,
        TColumnFilter(schema.Columns().size()),
        /* keyColumns =*/{},
        /* partitionTag =*/std::nullopt,
        /* trafficMeter =*/nullptr,
        /* bandwidthThrottler =*/GetUnlimitedThrottler(),
        /* rpsThrottler =*/GetUnlimitedThrottler(),
        /* multiReaderMemoryManager =*/readerMemoryManager);

    auto blockInputStream = CreateBlockInputStream(
        reader,
        schema,
        blockInputStreamTraceContext,
        queryContext->Bootstrap,
        TLogger(queryContext->Logger).AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId),
        prewhereInfo);
    return {blockInputStream, reader};
}

std::vector<TDataSliceDescriptor> GetFilteredDataSliceDescriptors(
    BlockInputStreamPtr blockInputStream,
    ISchemalessMultiChunkReaderPtr reader)
{
    std::vector<TDataSliceDescriptor> filteredDataSliceDescriptors;
    while (auto block = blockInputStream->read()) {
        if (block.rows() > 0) {
            filteredDataSliceDescriptors.emplace_back(reader->GetCurrentReaderDescriptor());
            reader->SkipCurrentReader();
        }
    }
    return filteredDataSliceDescriptors;
}

std::vector<std::vector<TDataSliceDescriptor>> FilterDataSliceDescriptorsByPrewhereInfo(
    std::vector<std::vector<TDataSliceDescriptor>>&& perThreadDataSliceDescriptors,
    PrewhereInfoPtr prewhereInfo,
    TQueryContext* queryContext,
    const TSubquerySpec& subquerySpec,
    const NTracing::TTraceContextPtr& traceContext)
{
    std::vector<NYT::TFuture<std::vector<TDataSliceDescriptor>>> asyncFilterResults;
    auto prewhereColumns = ExtractColumnsFromPrewhereInfo(prewhereInfo);

    auto Logger = queryContext->Logger;
    YT_LOG_DEBUG(
        "Started executing PREWHERE data slice filtering (PrewhereColumnName: %v, PrewhereColumns: %v)",
        prewhereInfo->prewhere_column_name,
        prewhereColumns);

    for (const auto& threadDataSliceDescriptors : perThreadDataSliceDescriptors) {
        auto [blockInputStream, reader] = CreateBlockInputStreamAndReader(
            queryContext,
            subquerySpec,
            prewhereColumns,
            traceContext,
            threadDataSliceDescriptors,
            prewhereInfo);

        asyncFilterResults.emplace_back(
            BIND(&GetFilteredDataSliceDescriptors, blockInputStream, reader)
                .AsyncVia(queryContext->Bootstrap->GetWorkerInvoker())
                .Run());
    }

    std::vector<std::vector<TDataSliceDescriptor>> filteredDataSliceDescriptorsPerThread;

    size_t droppedDataSliceCount = 0;
    for (auto& asyncFilterResult : asyncFilterResults) {
        auto filteredDataSliceDescriptors = WaitFor(asyncFilterResult)
            .ValueOrThrow();
        if (!filteredDataSliceDescriptors.empty()) {
            filteredDataSliceDescriptorsPerThread.emplace_back(
                std::move(filteredDataSliceDescriptors));
        } else {
            droppedDataSliceCount++;
        }
    }
    YT_LOG_DEBUG("Finished executing PREWHERE data slice filtration (DroppedDataSliceCount: %v)", droppedDataSliceCount);

    return filteredDataSliceDescriptorsPerThread;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TStorageSubquery : public DB::IStorage
{
public:
    TStorageSubquery(TQueryContext* queryContext, TSubquerySpec subquerySpec)
        : QueryContext_(queryContext), SubquerySpec_(std::move(subquerySpec))
    {
        if (SubquerySpec_.InitialQueryId != queryContext->QueryId) {
            queryContext->Logger.AddTag("InitialQueryId: %v", SubquerySpec_.InitialQueryId);
            if (queryContext->InitialQuery) {
                YT_VERIFY(*queryContext->InitialQuery == SubquerySpec_.InitialQuery);
            } else {
                queryContext->InitialQuery = SubquerySpec_.InitialQuery;
            }
        }
        Logger = queryContext->Logger;
        Logger.AddTag(
            "SubqueryIndex: %v, SubqueryTableIndex: %v",
            SubquerySpec_.SubqueryIndex,
            SubquerySpec_.TableIndex);

        setColumns(ColumnsDescription(ToNamesAndTypesList(SubquerySpec_.ReadSchema)));

        queryContext->MoveToPhase(EQueryPhase::Preparation);
    }

    std::string getName() const override
    {
        return "YT";
    }

    std::string getTableName() const override
    {
        return "Subquery";
    }

    std::string getDatabaseName() const override
    {
        return "";
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

    BlockInputStreams read(
        const Names& columnNames,
        const SelectQueryInfo& queryInfo,
        const Context& context,
        QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned maxStreamCount) override
    {
        QueryContext_->MoveToPhase(EQueryPhase::Execution);

        const auto& traceContext = GetQueryContext(context)->TraceContext;
        const auto& prewhereInfo = queryInfo.prewhere_info;

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
        traceContext->AddTag("chyt.row_count" + tableIndexSuffix, ToString(totalRowCount));
        traceContext->AddTag("chyt.data_weight" + tableIndexSuffix, ToString(totalDataWeight));
        traceContext->AddTag("chyt.data_slice_count" + tableIndexSuffix, ToString(totalDataSliceCount));
        if (SubquerySpec_.TableIndex == 0) {
            traceContext->AddTag("chyt.subquery_index", ToString(SubquerySpec_.SubqueryIndex));
        }

        if (prewhereInfo) {
            perThreadDataSliceDescriptors = NDetail::FilterDataSliceDescriptorsByPrewhereInfo(
                std::move(perThreadDataSliceDescriptors),
                prewhereInfo,
                QueryContext_,
                SubquerySpec_,
                traceContext);

            i64 filteredDataWeight = 0;
            for (const auto& threadDataSliceDescriptors : perThreadDataSliceDescriptors) {
                for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                    filteredDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                }
            }
            double droppedRate  = 1.0 - static_cast<double>(filteredDataWeight) / static_cast<double>(totalDataWeight);
            YT_LOG_DEBUG("PREWHERE filtration finished (DroppedRate: %v)", droppedRate);
        }

        YT_LOG_DEBUG(
            "Creating table readers (MaxStreamCount: %v, StripeCount: %v, Columns: %v)",
            maxStreamCount,
            SubquerySpec_.DataSliceDescriptors.size(),
            columnNames);

        auto schema = SubquerySpec_.ReadSchema;

        YT_LOG_INFO("Creating table readers");
        BlockInputStreams streams;

        for (int threadIndex = 0;
             threadIndex < static_cast<int>(perThreadDataSliceDescriptors.size());
             ++threadIndex)
        {
            const auto& threadDataSliceDescriptors = perThreadDataSliceDescriptors[threadIndex];
            streams.emplace_back(NDetail::CreateBlockInputStreamAndReader(
                QueryContext_,
                SubquerySpec_,
                columnNames,
                traceContext,
                threadDataSliceDescriptors,
                prewhereInfo)
                .first);

            i64 rowCount = 0;
            i64 dataWeight = 0;
            int dataSliceCount = 0;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                rowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
                dataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                ++dataSliceCount;
                for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                    // It is crucial for better memory estimation.
                    YT_VERIFY(FindProtoExtension<NChunkClient::NProto::TMiscExt>(
                        chunkSpec.chunk_meta().extensions()));
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

        return streams;
    }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context& /* context */) const override
    {
        return QueryProcessingStage::Enum::FetchColumns;
    }

private:
    TQueryContext* QueryContext_;
    TSubquerySpec SubquerySpec_;
    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr CreateStorageSubquery(TQueryContext* queryContext, TSubquerySpec subquerySpec)
{
    return std::make_shared<TStorageSubquery>(queryContext, std::move(subquerySpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
