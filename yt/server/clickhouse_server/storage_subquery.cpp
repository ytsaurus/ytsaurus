#include "storage_subquery.h"

#include "db_helpers.h"
#include "subquery_spec.h"
#include "type_helpers.h"
#include "query_context.h"
#include "block_input_stream.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TStorageSubquery
    : public DB::IStorage
{
public:
    TStorageSubquery(
        TQueryContext* queryContext,
        TSubquerySpec subquerySpec)
        : QueryContext_(queryContext)
        , SubquerySpec_(std::move(subquerySpec))
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
        Logger.AddTag("SubqueryIndex: %v, SubqueryTableIndex: %v", SubquerySpec_.SubqueryIndex, SubquerySpec_.TableIndex);

        setColumns(ColumnsDescription(SubquerySpec_.Columns));
    }

    std::string getName() const override { return "YT"; }

    std::string getTableName() const override { return "Subquery"; }

    std::string getDatabaseName() const override { return ""; }

    bool isRemote() const override
    {
        // NB: from CH point of view this is already a non-remote query.
        // If we return here, GLOBAL IN stops working: CHYT-117.
        return false;
    }

    BlockInputStreams read(
        const Names& columnNames,
        const SelectQueryInfo& /* queryInfo */,
        const Context& /* context */,
        QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned maxStreamCount) override
    {
        // TODO(max42): ?
        auto columns = ToString(columnNames);

        i64 totalRowCount = 0;
        i64 totalDataWeight = 0;
        i64 totalDataSliceCount = 0;
        for (const auto& threadDataSliceDescriptors : SubquerySpec_.DataSliceDescriptors) {
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                totalRowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
                totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                ++totalDataSliceCount;
            }
        }
        YT_LOG_DEBUG("Deserialized subquery spec (RowCount: %v, DataWeight: %v, DataSliceCount: %v)",
            totalRowCount,
            totalDataWeight,
            totalDataSliceCount);

        YT_LOG_DEBUG("Creating table readers (MaxStreamCount: %v, StripeCount: %v, Columns: %v)",
            maxStreamCount,
            SubquerySpec_.DataSliceDescriptors.size(),
            columns);

        auto schema = SubquerySpec_.ReadSchema;

        // TODO(max42): do we still need this?
        std::vector<TString> dataColumns;
        for (const auto& columnName : columns) {
            if (!schema.FindColumn(columnName)) {
                THROW_ERROR_EXCEPTION("Column not found")
                    << TErrorAttribute("column", columnName);
            }
            dataColumns.emplace_back(columnName);
        }

        auto readSchema = schema.Filter(dataColumns);

        YT_LOG_DEBUG("Narrowing schema to %v columns", readSchema.GetColumnCount());

        // TODO(max42): put something here :) And move to config.
        auto config = New<TTableReaderConfig>();
        config->GroupSize = 150_MB;
        config->WindowSize = 200_MB;
        auto options = New<NTableClient::TTableReaderOptions>();

        YT_LOG_INFO("Creating table readers");
        BlockInputStreams streams;

        for (int threadIndex = 0; threadIndex < static_cast<int>(SubquerySpec_.DataSliceDescriptors.size()); ++threadIndex) {
            const auto& threadDataSliceDescriptors = SubquerySpec_.DataSliceDescriptors[threadIndex];
            // TODO(max42): fill properly.
            TClientBlockReadOptions blockReadOptions;
            blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
            blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
            blockReadOptions.WorkloadDescriptor.CompressionFairShareTag = QueryContext_->User;
            blockReadOptions.ReadSessionId = NChunkClient::TReadSessionId::Create();

            i64 rowCount = 0;
            i64 dataWeight = 0;
            int dataSliceCount = 0;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                rowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
                dataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                ++dataSliceCount;
            }

            auto reader = CreateSchemalessParallelMultiReader(
                config,
                options,
                QueryContext_->Client(),
                {} /* localDescriptor */,
                std::nullopt,
                QueryContext_->Client()->GetNativeConnection()->GetBlockCache(),
                SubquerySpec_.NodeDirectory,
                SubquerySpec_.DataSourceDirectory,
                threadDataSliceDescriptors,
                TNameTable::FromSchema(readSchema),
                blockReadOptions,
                TColumnFilter(readSchema.Columns().size()),
                {}, /* keyColumns */
                std::nullopt /* partitionTag */,
                nullptr /* trafficMeter */,
                GetUnlimitedThrottler() /* bandwidthThrottler */,
                GetUnlimitedThrottler() /* rpsThrottler */);

            YT_LOG_DEBUG("Thread table reader created (ThreadIndex: %v, RowCount: %v, DataWeight: %v, DataSliceCount: %v)",
                threadIndex,
                rowCount,
                dataWeight,
                dataSliceCount);

            TStringBuilder debugString;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                debugString.AppendString(ToString(dataSliceDescriptor));
                debugString.AppendString("\n");
            }
            YT_LOG_DEBUG("Thread debug string (ThreadIndex: %v, DebugString: %v)", threadIndex, debugString.Flush());

            streams.emplace_back(CreateBlockInputStream(
                std::move(reader), 
                readSchema, 
                TLogger(Logger)
                    .AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId)));
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

StoragePtr CreateStorageSubquery(
    TQueryContext* queryContext,
    TSubquerySpec subquerySpec)
{
    return std::make_shared<TStorageSubquery>(
        queryContext,
        std::move(subquerySpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
