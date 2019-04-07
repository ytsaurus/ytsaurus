#include "storage_subquery.h"

#include "db_helpers.h"
#include "format_helpers.h"
#include "input_stream.h"
#include "storage_with_virtual_columns.h"
#include "subquery_spec.h"
#include "type_helpers.h"
#include "chunk_reader.h"
#include "virtual_columns.h"
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

#include <library/string_utils/base64/base64.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NChunkPools;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TStorageSubquery
    : public IStorageWithVirtualColumns
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

        YCHECK((SubquerySpec_.InitialQueryId == queryContext->QueryId) == (queryContext->QueryKind == EQueryKind::InitialQuery));
        if (SubquerySpec_.InitialQueryId != queryContext->QueryId) {
            queryContext->Logger.AddTag("InitialQueryId: %v", SubquerySpec_.InitialQueryId);
        }

        auto representative = SubquerySpec_.GetTables().front();
        Columns_ = GetTableColumns(*representative);

        setColumns(ColumnsDescription(Columns_));
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
        const auto& Logger = QueryContext_->Logger;

        DB::Names physicalColumns;
        DB::Names virtualColumns;
        SplitColumns(columnNames, physicalColumns, virtualColumns);

        auto columns = ToString(physicalColumns);
        auto systemColumns = GetSystemColumns(virtualColumns);

        i64 totalRowCount = 0;
        i64 totalDataWeight = 0;
        for (const auto& dataSliceDescriptor : SubquerySpec_.DataSliceDescriptors) {
            totalRowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
            totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
        }
        YT_LOG_DEBUG("Deserialized subquery spec (RowCount: %v, DataWeight: %v)", totalRowCount, totalDataWeight);

        auto dataSourceType = SubquerySpec_.GetCommonDataSourceType();
        YT_LOG_DEBUG("Creating table readers (MaxStreamCount: %v, DataSourceType: %v, Columns: %v)",
            maxStreamCount,
            dataSourceType,
            columns);

        TChunkStripeListPtr chunkStripeList;
        if (dataSourceType == EDataSourceType::UnversionedTable) {
            std::vector<TInputDataSlicePtr> dataSlices;
            for (const auto& dataSliceDescriptor : SubquerySpec_.DataSliceDescriptors) {
                const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();
                auto inputChunk = New<TInputChunk>(chunkSpec);
                auto inputChunkSlice = New<TInputChunkSlice>(std::move(inputChunk));
                auto dataSlice = CreateUnversionedInputDataSlice(std::move(inputChunkSlice));
                dataSlices.emplace_back(std::move(dataSlice));
            }
            chunkStripeList = BuildJobs(dataSlices, maxStreamCount);
        } else {
            YCHECK(false);
        }

        i64 totalDataSliceCount = 0;
        for (const auto& stripe : chunkStripeList->Stripes) {
            totalDataSliceCount += stripe->DataSlices.size();
        }
        YT_LOG_DEBUG("Per-thread stripes prepared (Count: %v, TotalDataSliceCount: %v)", chunkStripeList->Stripes.size(), totalDataSliceCount);

        auto schema = SubquerySpec_.GetCommonNativeSchema();

        std::vector<TString> dataColumns;
        for (const auto& columnName : columns) {
            if (!schema.FindColumn(columnName)) {
                THROW_ERROR_EXCEPTION("Column not found")
                    << TErrorAttribute("column", columnName);
            }
            dataColumns.emplace_back(columnName);
        }

        auto readerSchema = schema.Filter(dataColumns);

        std::vector<TClickHouseTablePtr> sourceTables = SubquerySpec_.GetTables();

        YT_LOG_DEBUG("Number of job source tables: %v", sourceTables.size());

        std::vector<TClickHouseTablePtr> readerTables;
        readerTables.reserve(sourceTables.size());
        for (const auto& table : sourceTables) {
            auto filtered = std::make_shared<TClickHouseTable>(table->Name);

            for (const auto& column: table->Columns) {
                if (IsIn(columns, column.Name)) {
                    filtered->Columns.push_back(column);
                }
            }

            readerTables.emplace_back(std::move(filtered));
        }

        YT_LOG_DEBUG("Narrowing schema to %v physical columns", readerSchema.GetColumnCount());

        // TODO
        auto nativeReaderConfig = New<TTableReaderConfig>();
        auto nativeReaderOptions = New<NTableClient::TTableReaderOptions>();

        if (systemColumns.TableName) {
            nativeReaderOptions->EnableTableIndex = true;
        }

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
                readerSchema,
                true);

            chunkReaders.push_back(std::move(chunkReader));
        }

        // It seems there is no need to warm up readers
        // WarmUp(chunkReaders);

        auto readerColumns = readerTables.front()->Columns;

        TTableReaderList readers;
        for (auto& chunkReader : chunkReaders) {
            readers.emplace_back(CreateTableReader(
                readerTables,
                readerColumns,
                systemColumns,
                std::move(chunkReader)));
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
    NamesAndTypesList Columns_;
    TSubquerySpec SubquerySpec_;

    const NamesAndTypesList& ListPhysicalColumns() const override
    {
        return Columns_;
    }

    const NamesAndTypesList& ListVirtualColumns() const override
    {
        return ListSystemVirtualColumns();
    }
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
