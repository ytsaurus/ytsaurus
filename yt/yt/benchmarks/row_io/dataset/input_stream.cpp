#include "input_stream.h"

#include "dataset.h"
#include "private.h"

#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/adapters.h>

#include <yt/yt/library/formats/arrow_writer.h>
#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>

#include <yt/yt/core/misc/blob_output.h>

namespace NYT::NRowIOBenchmark {

using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateInputStreamWriter(
    const TFormat& format,
    const TNameTablePtr& nameTable,
    const TTableSchemaPtr& schema,
    IAsyncOutputStreamPtr output,
    TArrowWriterStatisticsPtr statistics)
{
    auto controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableEndOfStream = format.GetType() == EFormatType::Arrow;
    if (format.GetType() == EFormatType::Arrow) {
        return CreateWriterForArrow(
            format.Attributes(),
            nameTable,
            {schema},
            /*columns*/ {std::nullopt},
            std::move(output),
            /*enableContextSaving*/ false,
            controlAttributes,
            /*keyColumnCount*/ 0,
            std::move(statistics));
    }
    return CreateStaticTableWriterForFormat(
        format,
        nameTable,
        {schema},
        /*columns*/ {std::nullopt},
        std::move(output),
        /*enableContextSaving*/ false,
        controlAttributes,
        /*keyColumnCount*/ 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSharedRef PrepareInputStream(
    const TFormat& format,
    const TDataset& dataset,
    int batchRowCount,
    TArrowWriterStatisticsPtr statistics)
{
    auto reader = CreateReader(dataset.Schema, dataset.Source);
    TBlobOutput output;
    auto writer = CreateInputStreamWriter(
        format,
        reader->GetNameTable(),
        dataset.Schema,
        CreateAsyncAdapter(&output),
        std::move(statistics));
    TPipeReaderToWriterByBatchesOptions options;
    options.StartingOptions.Columnar = format.GetType() == EFormatType::Arrow;
    options.StartingOptions.MaxRowsPerRead = batchRowCount;
    options.StartingOptions.MaxDataWeightPerRead = 16_MB;
    PipeReaderToWriterByBatches(
        CreateApiFromSchemalessChunkReaderAdapter(std::move(reader)),
        writer,
        options);
    return output.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
