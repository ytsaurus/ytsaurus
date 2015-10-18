#include "helpers.h"
#include "config.h"
#include "schemaless_chunk_reader.h"
#include "private.h"

#include "schemaless_reader.h"
#include "schemaless_writer.h"
#include "name_table.h"

#include <yt/ytlib/formats/parser.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NFormats;
using namespace NYson;

using NChunkClient::TChannel;

//////////////////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(const TFormat& format, NYson::IYsonConsumer* consumer)
    : Parser_(CreateParserForFormat(format, EDataType::Tabular, consumer))
{ }

TTableOutput::~TTableOutput() throw() = default;

void TTableOutput::DoWrite(const void* buf, size_t len)
{
    YCHECK(IsParserValid_);
    try {
        Parser_->Read(TStringBuf(static_cast<const char*>(buf), len));
    } catch (const std::exception& ex) {
        IsParserValid_ = false;
        throw;
    }
}

void TTableOutput::DoFinish()
{
    if (IsParserValid_) {
        // Dump everything into consumer.
        Parser_->Finish();
    }
}

//////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    ISchemalessReaderPtr reader,
    ISchemalessWriterPtr writer,
    int bufferRowCount,
    bool validateValues)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(bufferRowCount);

    while (reader->Read(&rows)) {
        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        if (validateValues) {
            for (const auto& row : rows) {
                for (const auto& value : row) {
                    ValidateStaticValue(value);
                }
            }
        }

        if (!writer->Write(rows)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    WaitFor(writer->Close())
        .ThrowOnError();

    YCHECK(rows.empty());
}

void PipeInputToOutput(
    TInputStream* input,
    TOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    TBlob buffer(TWriteBufferTag(), bufferBlockSize);

    while (true) {
        size_t length = input->Read(buffer.Begin(), buffer.Size());
        if (length == 0)
            break;

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}

//////////////////////////////////////////////////////////////////////////////////

// NB: not using TYsonString here to avoid copying.
TUnversionedValue MakeUnversionedValue(const TStringBuf& ysonString, int id, TStatelessLexer& lexer)
{
    TToken token;
    lexer.GetToken(ysonString, &token);
    YCHECK(!token.IsEmpty());

    switch (token.GetType()) {
        case ETokenType::Int64:
            return MakeUnversionedInt64Value(token.GetInt64Value(), id);

        case ETokenType::Uint64:
            return MakeUnversionedUint64Value(token.GetUint64Value(), id);

        case ETokenType::String:
            return MakeUnversionedStringValue(token.GetStringValue(), id);

        case ETokenType::Double:
            return MakeUnversionedDoubleValue(token.GetDoubleValue(), id);

        case ETokenType::Hash:
            return MakeUnversionedSentinelValue(EValueType::Null, id);

        default:
            return MakeUnversionedAnyValue(ysonString, id);
    }
}

//////////////////////////////////////////////////////////////////////////////////

int GetSystemColumnCount(TChunkReaderOptionsPtr options)
{
    int systemColumnCount = 0;
    if (options->EnableRowIndex) {
        ++systemColumnCount;
    }

    if (options->EnableRangeIndex) {
        ++systemColumnCount;
    }

    if (options->EnableTableIndex) {
        ++systemColumnCount;
    }

    return systemColumnCount;
}

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns)
{
    if (chunkKeyColumns.size() < keyColumns.size()) {
        THROW_ERROR_EXCEPTION("Chunk has less key columns than requested: actual %v, expected %v",
            chunkKeyColumns,
            keyColumns);
    }

    for (int i = 0; i < keyColumns.size(); ++i) {
        if (chunkKeyColumns[i] != keyColumns[i]) {
            THROW_ERROR_EXCEPTION("Incompatible key columns: actual %v, expected %v",
                chunkKeyColumns,
                keyColumns);
        }
    }
}

TColumnFilter CreateColumnFilter(const NChunkClient::TChannel& channel, TNameTablePtr nameTable)
{
    TColumnFilter columnFilter;
    if (channel.IsUniversal()) {
        return columnFilter;
    }

    // Ranges are not supported since 0.17
    YCHECK(channel.GetRanges().empty());

    columnFilter.All = false;
    for (auto column : channel.GetColumns()) {
        auto id = nameTable->GetIdOrRegisterName(column);
        columnFilter.Indexes.push_back(id);
    }

    return columnFilter;
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient
