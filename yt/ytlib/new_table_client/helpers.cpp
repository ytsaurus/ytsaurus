#include "stdafx.h"

#include "helpers.h"
#include "config.h"

#include "schemaless_reader.h"
#include "schemaless_writer.h"
#include "schemaless_chunk_reader.h"

#include <ytlib/formats/parser.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NFormats;
using namespace NYson;

//////////////////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(const TFormat& format, NYson::IYsonConsumer* consumer)
    : Parser_(CreateParserForFormat(format, EDataType::Tabular, consumer))
{ }

TTableOutput::~TTableOutput() throw()
{ }

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
                for (int i = 0; i < row.GetCount(); ++i) {
                    ValidateStaticValue(row[i]);
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

void PipeReaderToWriter(
    ISchemalessMultiChunkReaderPtr reader,
    NFormats::ISchemalessFormatWriterPtr writer,
    NVersionedTableClient::TControlAttributesConfigPtr config,
    int bufferRowCount,
    bool validateValues)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(bufferRowCount);

    int tableIndex = -1;
    i32 rangeIndex = -1;

    while (reader->Read(&rows)) {
        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        if (validateValues) {
            for (const auto& row : rows) {
                for (int i = 0; i < row.GetCount(); ++i) {
                    ValidateStaticValue(row[i]);
                }
            }
        }

        if (config->EnableTableIndex) {
            auto newTableIndex = reader->GetTableIndex();
            if (tableIndex != newTableIndex) {
                writer->WriteTableIndex(newTableIndex);
                tableIndex = newTableIndex;
            }
        }

        if (config->EnableRangeIndex) {
            auto newRangeIndex = reader->GetRangeIndex();
            if (rangeIndex != newRangeIndex) {
                writer->WriteRangeIndex(newRangeIndex);
                rangeIndex = newRangeIndex;
            }
        }

        if (config->EnableRowIndex) {
            writer->WriteRowIndex(reader->GetTableRowIndex() - rows.size());
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


} // namespace NYT
} // namespace NVersionedTableClient
