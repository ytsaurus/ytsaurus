#include "read_job.h"

#include "chunk_reader.h"
#include "data_slice.h"
#include "read_job_spec.h"
#include "table_reader.h"
#include "table_schema.h"

#include "private.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/core/misc/error.h>
#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NApi;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

static const NLogging::TLogger& Logger = ServerLogger;

namespace {

////////////////////////////////////////////////////////////////////////////////

TTablePtr FilterColumns(
    const TTablePtr& table,
    const std::vector<TString>& columns)
{
    auto filtered = std::make_shared<TTable>(table->Name);

    for (const auto& column: table->Columns) {
        if (IsIn(columns, column.Name)) {
            filtered->Columns.push_back(column);
        }
    }

    return filtered;
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableReaderOptionsPtr CreateNativeReaderOptions(
    const TTableReaderOptions& options,
    const TSystemColumns& systemColumns)
{
    auto nativeOptions = New<NTableClient::TTableReaderOptions>();

    Y_UNUSED(options);

    if (systemColumns.TableName.Defined()) {
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
    const NConcurrency::IThroughputThrottlerPtr throttler,
    size_t maxStreamCount,
    const TTableReaderOptions& options)
{
    auto readJobSpec = LoadReadJobSpec(jobSpec);

    auto dataSourceType = readJobSpec.GetCommonDataSourceType();
    LOG_DEBUG("Creating table readers (MaxStreamCount: %v, DataSourceType: %v, Columns: %v)",
        maxStreamCount,
        dataSourceType,
        columns);

    // TODO(max42): YT-9180.
    maxStreamCount = 1;

    std::vector<TDataSliceDescriptorList> dataSlices;

    if (dataSourceType == EDataSourceType::UnversionedTable) {
        dataSlices = MergeUnversionedChunks(
            std::move(readJobSpec.DataSliceDescriptors),
            maxStreamCount);
    } else if (dataSourceType == EDataSourceType::VersionedTable) {
        dataSlices = MergeVersionedChunks(
            std::move(readJobSpec.DataSliceDescriptors),
            maxStreamCount);
    } else {
        THROW_ERROR_EXCEPTION("Invalid job specification: unsupported data source type")
            << TErrorAttribute("type", dataSourceType);
    }

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

    TTableList sourceTables = readJobSpec.GetTables();

    LOG_DEBUG("Number of job source tables: %v", sourceTables.size());

    TTableList readerTables;
    readerTables.reserve(sourceTables.size());
    for (const auto& table : sourceTables) {
        readerTables.push_back(FilterColumns(table, dataColumns));
    }

    LOG_DEBUG("Narrowing schema to %v physical columns", readerSchema.GetColumnCount());

    // TODO
    auto nativeReaderConfig = New<TTableReaderConfig>();
    auto nativeReaderOptions = CreateNativeReaderOptions(options, systemColumns);

    LOG_INFO("Creating table readers");

    std::vector<NTableClient::ISchemafulReaderPtr> chunkReaders;
    for (const auto& dataSliceDescriptors: dataSlices) {
        auto chunkReader = CreateChunkReader(
            nativeReaderConfig,
            nativeReaderOptions,
            client,
            readJobSpec.NodeDirectory,
            readJobSpec.DataSourceDirectory,
            dataSliceDescriptors,
            throttler,
            readerSchema,
            options.Unordered);

        chunkReaders.push_back(std::move(chunkReader));
    }

    // It seems there is no need to warm up readers
    // WarmUp(chunkReaders);

    auto readerColumns = readerTables.front()->Columns;

    TTableReaderList readers;
    for (auto& chunkReader : chunkReaders) {
        readers.emplace_back(NNative::CreateTableReader(
            readerTables,
            readerColumns,
            systemColumns,
            std::move(chunkReader)));
    }
    return readers;
}

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
