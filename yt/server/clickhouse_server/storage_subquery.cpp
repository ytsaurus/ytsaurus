#include "storage_subquery.h"

#include "db_helpers.h"
#include "format_helpers.h"
#include "subquery_spec.h"
#include "type_helpers.h"
#include "subquery.h"
#include "query_context.h"
#include "table.h"
#include "block_input_stream.h"

#include <yt/server/controller_agent/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <library/string_utils/base64/base64.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NChunkPools;
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
        std::string subquerySpec)
        : QueryContext_(queryContext)
    {
        auto base64DecodedSpec = Base64Decode(subquerySpec);
        NProto::TSubquerySpec protoSpec;
        protoSpec.ParseFromString(base64DecodedSpec);
        SubquerySpec_ = NYT::FromProto<TSubquerySpec>(protoSpec);

        if (SubquerySpec_.InitialQueryId != queryContext->QueryId) {
            queryContext->Logger.AddTag("InitialQueryId: %v", SubquerySpec_.InitialQueryId);
        }

        setColumns(ColumnsDescription(SubquerySpec_.Columns));
    }

    std::string getName() const override { return "YT"; }

    std::string getTableName() const override { return "Subquery"; }

    bool isRemote() const override { return true; }

    BlockInputStreams read(
        const Names& columnNames,
        const SelectQueryInfo& /* queryInfo */,
        const Context& /* context */,
        QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned maxStreamCount) override
    {
        // TODO(max42): normal logging in this method.
        const auto& Logger = QueryContext_->Logger;

        // TODO(max42): ?
        auto columns = ToString(columnNames);

        i64 totalRowCount = 0;
        i64 totalDataWeight = 0;
        for (const auto& dataSliceDescriptor : SubquerySpec_.DataSliceDescriptors) {
            totalRowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
            totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
        }
        YT_LOG_DEBUG("Deserialized subquery spec (RowCount: %v, DataWeight: %v)", totalRowCount, totalDataWeight);

        YT_LOG_DEBUG("Creating table readers (MaxStreamCount: %v, Columns: %v)", maxStreamCount, columns);

        TChunkStripeListPtr chunkStripeList;
        std::vector<TInputDataSlicePtr> dataSlices;
        for (const auto& dataSliceDescriptor : SubquerySpec_.DataSliceDescriptors) {
            const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();
            auto inputChunk = New<TInputChunk>(chunkSpec);
            auto inputChunkSlice = New<TInputChunkSlice>(std::move(inputChunk));
            auto dataSlice = CreateUnversionedInputDataSlice(std::move(inputChunkSlice));
            dataSlices.emplace_back(std::move(dataSlice));
        }
        chunkStripeList = SubdivideDataSlices(dataSlices, maxStreamCount);

        i64 totalDataSliceCount = 0;
        for (const auto& stripe : chunkStripeList->Stripes) {
            totalDataSliceCount += stripe->DataSlices.size();
        }
        YT_LOG_DEBUG("Per-thread stripes prepared (Count: %v, TotalDataSliceCount: %v)", chunkStripeList->Stripes.size(), totalDataSliceCount);

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

        for (const auto& chunkStripe : chunkStripeList->Stripes) {
            std::vector<TDataSliceDescriptor> dataSliceDescriptors;
            i64 rowCount = 0;
            i64 dataWeight = 0;
            for (const auto& inputDataSlice : chunkStripe->DataSlices) {
                auto inputChunk = inputDataSlice->GetSingleUnversionedChunkOrThrow();
                NChunkClient::NProto::TChunkSpec chunkSpec;
                auto chunkSlice = inputDataSlice->ChunkSlices[0];
                if (chunkSlice->UpperLimit().RowIndex) {
                    inputChunk->UpperLimit() = std::make_unique<TReadLimit>();
                    inputChunk->UpperLimit()->SetRowIndex(*chunkSlice->UpperLimit().RowIndex);
                }
                if (chunkSlice->LowerLimit().RowIndex) {
                    inputChunk->LowerLimit() = std::make_unique<TReadLimit>();
                    inputChunk->LowerLimit()->SetRowIndex(*chunkSlice->LowerLimit().RowIndex);
                }
                ToProto(&chunkSpec, inputChunk, EDataSourceType::UnversionedTable);
                chunkSpec.set_row_count_override(inputDataSlice->GetRowCount());
                chunkSpec.set_data_weight_override(inputDataSlice->GetDataWeight());
                dataSliceDescriptors.emplace_back(chunkSpec);

                rowCount += inputChunk->GetRowCount();
                dataWeight += inputDataSlice->GetDataWeight();
            }

            // TODO(max42): fill properly.
            TClientBlockReadOptions blockReadOptions;
            blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
            blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
            blockReadOptions.WorkloadDescriptor.CompressionFairShareTag = QueryContext_->User;
            blockReadOptions.ReadSessionId = NChunkClient::TReadSessionId::Create();

            auto reader = CreateSchemalessParallelMultiReader(
                config,
                options,
                QueryContext_->Client(),
                {} /* localDescriptor */,
                std::nullopt,
                QueryContext_->Client()->GetNativeConnection()->GetBlockCache(),
                SubquerySpec_.NodeDirectory,
                SubquerySpec_.DataSourceDirectory,
                dataSliceDescriptors,
                TNameTable::FromSchema(readSchema),
                blockReadOptions,
                TColumnFilter(readSchema.Columns().size()),
                {}, /* keyColumns */
                std::nullopt /* partitionTag */,
                nullptr /* trafficMeter */,
                GetUnlimitedThrottler() /* bandwidthThrottler */,
                GetUnlimitedThrottler() /* rpsThrottler */);

            YT_LOG_DEBUG("Table reader created (RowCount: %v, DataWeight: %v)", rowCount, dataWeight);

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
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr CreateStorageSubquery(
    TQueryContext* queryContext,
    std::string subquerySpec)
{
    return std::make_shared<TStorageSubquery>(
        queryContext,
        std::move(subquerySpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
