#include "read_job.h"

#include "job_input.h"
#include "chunk_reader.h"
#include "read_job_spec.h"
#include "table_reader.h"
#include "table_schema.h"

#include "private.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/core/misc/error.h>
#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NApi;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

static const NLogging::TLogger& Logger = ServerLogger;

namespace {

////////////////////////////////////////////////////////////////////////////////

TClickHouseTablePtr FilterColumns(
    const TClickHouseTablePtr& table,
    const std::vector<TString>& columns)
{
    auto filtered = std::make_shared<TClickHouseTable>(table->Name);

    for (const auto& column: table->Columns) {
        if (IsIn(columns, column.Name)) {
            filtered->Columns.push_back(column);
        }
    }

    return filtered;
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableReaderOptionsPtr CreateNativeReaderOptions(const TSystemColumns& systemColumns)
{
    auto nativeOptions = New<NTableClient::TTableReaderOptions>();

    if (systemColumns.TableName) {
        nativeOptions->EnableTableIndex = true;
    }

    return nativeOptions;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TReadJobSpec LoadReadJobSpec(const TString& spec)
{
    auto asNode = ConvertToNode(TYsonString(spec));
    auto readJobSpec = ConvertTo<TReadJobSpec>(asNode);

    readJobSpec.Validate();

    return readJobSpec;
}

////////////////////////////////////////////////////////////////////////////////

TTableReaderList CreateJobTableReaders(
    const NApi::NNative::IClientPtr& client,
    const TString& jobSpec,
    const std::vector<TString>& columns,
    const TSystemColumns& systemColumns,
    size_t maxStreamCount,
    bool unordered)
{
    auto readJobSpec = LoadReadJobSpec(jobSpec);

    i64 totalRowCount = 0;
    i64 totalDataWeight = 0;
    for (const auto& dataSliceDescriptor : readJobSpec.DataSliceDescriptors) {
        totalRowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
        totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
    }
    YT_LOG_DEBUG("Deserialized job spec (RowCount: %v, DataWeight: %v)", totalRowCount, totalDataWeight);

    auto dataSourceType = readJobSpec.GetCommonDataSourceType();
    YT_LOG_DEBUG("Creating table readers (MaxStreamCount: %v, DataSourceType: %v, Columns: %v)",
        maxStreamCount,
        dataSourceType,
        columns);

    TChunkStripeListPtr chunkStripeList;
    if (dataSourceType == EDataSourceType::UnversionedTable) {
        std::vector<TInputDataSlicePtr> dataSlices;
        for (const auto& dataSliceDescriptor : readJobSpec.DataSliceDescriptors) {
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

    auto schema = readJobSpec.GetCommonNativeSchema();

    std::vector<TString> dataColumns;
    for (const auto& columnName : columns) {
        if (!schema.FindColumn(columnName)) {
            THROW_ERROR_EXCEPTION("Column not found")
                << TErrorAttribute("column", columnName);
        }
        dataColumns.emplace_back(columnName);
    }

    auto readerSchema = schema.Filter(dataColumns);

    std::vector<TClickHouseTablePtr> sourceTables = readJobSpec.GetTables();

    YT_LOG_DEBUG("Number of job source tables: %v", sourceTables.size());

    std::vector<TClickHouseTablePtr> readerTables;
    readerTables.reserve(sourceTables.size());
    for (const auto& table : sourceTables) {
        readerTables.push_back(FilterColumns(table, dataColumns));
    }

    YT_LOG_DEBUG("Narrowing schema to %v physical columns", readerSchema.GetColumnCount());

    // TODO
    auto nativeReaderConfig = New<TTableReaderConfig>();
    auto nativeReaderOptions = CreateNativeReaderOptions(systemColumns);

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
            client,
            readJobSpec.NodeDirectory,
            readJobSpec.DataSourceDirectory,
            dataSliceDescriptors,
            GetUnlimitedThrottler(),
            readerSchema,
            unordered);

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
    return readers;
}

} // namespace NYT::NClickHouseServer
