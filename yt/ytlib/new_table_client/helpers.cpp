#include "stdafx.h"

#include "helpers.h"

#include "schemaless_reader.h"
#include "schemaless_writer.h"

#include <ytlib/formats/parser.h>

#include <core/concurrency/scheduler.h>


namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NFormats;

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
    bool validateDoubles)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(bufferRowCount);

    while (reader->Read(&rows)) {
        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        if (validateDoubles) {
            for (const auto& row : rows) {
                for (int i = 0; i < row.GetCount(); ++i) {
                    if (!IsValidValue(row[i])) {
                        THROW_ERROR_EXCEPTION(
                            EErrorCode::InvalidDoubleValue, 
                            "Invalid double value %Qv", 
                            row[i]);
                    }
                }
            }
        }

        if (!writer->Write(rows)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

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

} // namespace NYT
} // namespace NVersionedTableClient
