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
namespace NClickHouse {

using namespace NYT::NApi;
using namespace NYT::NChunkClient;
using namespace NYT::NNodeTrackerClient;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

static const NLogging::TLogger& Logger = ServerLogger;

namespace {

////////////////////////////////////////////////////////////////////////////////

NInterop::TTablePtr FilterColumns(
    const NInterop::TTablePtr& table,
    const std::vector<TString>& columns)
{
    auto filtered = std::make_shared<NInterop::TTable>(table->Name);

    for (const auto& column: table->Columns) {
        if (IsIn(columns, column.Name)) {
            filtered->Columns.push_back(column);
        }
    }

    return filtered;
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableReaderOptionsPtr CreateNativeReaderOptions(
    const NInterop::TTableReaderOptions& options,
    const NInterop::TSystemColumns& systemColumns)
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

NInterop::TTableReaderList CreateJobTableReaders(
    const NNative::IClientPtr& client,
    const TString& jobSpec,
    const NInterop::TStringList& columns,
    const NInterop::TSystemColumns& systemColumns,
    const NConcurrency::IThroughputThrottlerPtr throttler,
    size_t maxStreamCount,
    const NInterop::TTableReaderOptions& options)
{
    auto readJobSpec = LoadReadJobSpec(jobSpec);

    auto dataSourceType = readJobSpec.GetCommonDataSourceType();
    LOG_DEBUG("Creating table readers (MaxStreamCount: %v, DataSourceType: %v, Columns: %v)",
        maxStreamCount,
        dataSourceType,
        columns);

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

    NInterop::TStringList dataColumns;
    for (const auto& columnName : columns) {
        if (!schema.FindColumn(columnName)) {
            THROW_ERROR_EXCEPTION("Column not found")
                << TErrorAttribute("column", columnName);
        }
        dataColumns.emplace_back(columnName);
    }

    auto readerSchema = schema.Filter(dataColumns);

    NInterop::TTableList sourceTables = readJobSpec.GetTables();

    LOG_DEBUG("Number of job source tables: %v", sourceTables.size());

    NInterop::TTableList readerTables;
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

    NInterop::TTableReaderList readers;
    for (auto& chunkReader : chunkReaders) {
        readers.emplace_back(NClickHouse::CreateTableReader(
            readerTables,
            readerColumns,
            systemColumns,
            std::move(chunkReader)));
    }
    return readers;
}

}   // namespace NClickHouse
}   // namespace NYT
