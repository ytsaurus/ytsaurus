#include "storage_subquery.h"

#include "db_helpers.h"
#include "format_helpers.h"
#include "input_stream.h"
#include "subquery_spec.h"
#include "type_helpers.h"
#include "chunk_reader.h"
#include "subquery.h"
#include "query_context.h"
#include "table_reader.h"
#include "table.h"

#include <yt/server/controller_agent/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>

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

        YT_VERIFY((SubquerySpec_.InitialQueryId == queryContext->QueryId) == (queryContext->QueryKind == EQueryKind::InitialQuery));
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

        // TODO
        auto nativeReaderConfig = New<TTableReaderConfig>();
        auto nativeReaderOptions = New<NTableClient::TTableReaderOptions>();

        YT_LOG_INFO("Creating table readers");

        std::vector<NTableClient::ISchemafulReaderPtr> chunkReaders;
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

            YT_LOG_DEBUG("Table reader created (RowCount: %v, DataWeight: %v)", rowCount, dataWeight);

            auto chunkReader = CreateChunkReader(
                nativeReaderConfig,
                nativeReaderOptions,
                QueryContext_->Client(),
                SubquerySpec_.NodeDirectory,
                SubquerySpec_.DataSourceDirectory,
                dataSliceDescriptors,
                GetUnlimitedThrottler(),
                readSchema,
                true);

            chunkReaders.push_back(std::move(chunkReader));
        }

        // It seems there is no need to warm up readers
        // WarmUp(chunkReaders);

        // TODO(max42): refactor.
        auto foo = std::make_shared<TClickHouseTable>("foo", readSchema);

        TTableReaderList readers;
        for (auto& chunkReader : chunkReaders) {
            readers.emplace_back(CreateTableReader(foo->Columns, std::move(chunkReader)));
        }

        BlockInputStreams streams;
        for (auto& tableReader : readers) {
            streams.emplace_back(CreateStorageInputStream(std::move(tableReader)));
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
