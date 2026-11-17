#include "input_stream.h"

#include "dataset.h"
#include "private.h"

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

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

void CheckArrowStatus(const arrow20::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Failed to prepare dense Arrow input")
            .With("arrow_error", status.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T UnwrapArrowResult(arrow20::Result<T> result)
{
    CheckArrowStatus(result.status());
    return std::move(result).ValueOrDie();
}

////////////////////////////////////////////////////////////////////////////////

void VerifyNoDictionaries(const std::shared_ptr<arrow20::DataType>& type)
{
    YT_VERIFY(type->id() != arrow20::Type::DICTIONARY);
    for (const auto& field : type->fields()) {
        VerifyNoDictionaries(field->type());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::Schema> MakeDenseSchema(const arrow20::Schema& schema)
{
    arrow20::FieldVector fields;
    fields.reserve(schema.num_fields());
    for (const auto& field : schema.fields()) {
        auto type = field->type();
        if (type->id() == arrow20::Type::DICTIONARY) {
            type = std::static_pointer_cast<arrow20::DictionaryType>(type)->value_type();
        }
        VerifyNoDictionaries(type);
        fields.push_back(field->WithType(type));
    }
    return arrow20::schema(std::move(fields), schema.metadata());
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::RecordBatch> MakeDenseBatch(
    const arrow20::RecordBatch& batch,
    const std::shared_ptr<arrow20::Schema>& schema)
{
    arrow20::ArrayVector columns;
    columns.reserve(batch.num_columns());
    for (int index = 0; index < batch.num_columns(); ++index) {
        auto column = batch.column(index);
        if (column->type_id() == arrow20::Type::DICTIONARY) {
            column = UnwrapArrowResult(arrow20::compute::Cast(
                column,
                schema->field(index)->type())).make_array();
        }
        columns.push_back(std::move(column));
    }
    auto result = arrow20::RecordBatch::Make(schema, batch.num_rows(), std::move(columns));
    CheckArrowStatus(result->ValidateFull());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef RemoveArrowDictionaries(
    const TSharedRef& input,
    const TArrowWriterStatisticsPtr& statistics)
{
    auto buffer = std::make_shared<arrow20::Buffer>(
        reinterpret_cast<const ui8*>(input.Begin()),
        input.Size());
    arrow20::io::BufferReader source(buffer);
    auto output = UnwrapArrowResult(arrow20::io::BufferOutputStream::Create());
    while (UnwrapArrowResult(source.Tell()) < std::ssize(input)) {
        auto reader = UnwrapArrowResult(arrow20::ipc::RecordBatchStreamReader::Open(&source));
        auto schema = MakeDenseSchema(*reader->schema());
        auto writer = UnwrapArrowResult(arrow20::ipc::MakeStreamWriter(output, schema));
        while (true) {
            std::shared_ptr<arrow20::RecordBatch> batch;
            CheckArrowStatus(reader->ReadNext(&batch));
            if (!batch) {
                break;
            }
            auto denseBatch = MakeDenseBatch(*batch, schema);
            CheckArrowStatus(writer->WriteRecordBatch(*denseBatch));
            if (statistics) {
                ++statistics->RecordBatchCount;
                statistics->ColumnBatchCounts[EArrowEncoding::Dense] += batch->num_columns();
            }
        }
        CheckArrowStatus(writer->Close());
    }
    struct TDenseArrowInputTag
    { };
    auto result = UnwrapArrowResult(output->Finish());
    return TSharedRef::MakeCopy<TDenseArrowInputTag>(TRef(result->data(), result->size()));
}

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
    TArrowWriterStatisticsPtr statistics,
    EArrowDictionaryEncoding dictionaryEncoding)
{
    auto disableDictionaries = dictionaryEncoding == EArrowDictionaryEncoding::Disabled;
    YT_VERIFY(!disableDictionaries || format.GetType() == EFormatType::Arrow);
    auto reader = CreateReader(dataset.Schema, dataset.Source);
    TBlobOutput output;
    auto writer = CreateInputStreamWriter(
        format,
        reader->GetNameTable(),
        dataset.Schema,
        CreateAsyncAdapter(&output),
        disableDictionaries ? nullptr : statistics);
    TPipeReaderToWriterByBatchesOptions options;
    options.StartingOptions.Columnar = format.GetType() == EFormatType::Arrow;
    options.StartingOptions.MaxRowsPerRead = batchRowCount;
    options.StartingOptions.MaxDataWeightPerRead = 16_MB;
    PipeReaderToWriterByBatches(
        CreateApiFromSchemalessChunkReaderAdapter(std::move(reader)),
        writer,
        options);
    auto input = output.Flush();
    return disableDictionaries ? RemoveArrowDictionaries(input, statistics) : input;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
