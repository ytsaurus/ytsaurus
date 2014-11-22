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
    : Consumer_(consumer)
    , Parser_(CreateParserForFormat(format, EDataType::Tabular, consumer))
    , IsParserValid_(true)
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

void PipeReaderToWriter(ISchemalessReaderPtr reader, ISchemalessWriterPtr writer, int bufferRowCount)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(bufferRowCount);

    while (reader->Read(&rows)) {
        if (rows.empty()) {
            auto error = WaitFor(reader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
            continue;
        }

        if (!writer->Write(rows)) {
            auto error = WaitFor(writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
    }
    YCHECK(rows.empty());
}

void PipeInputToOutput(
    TInputStream* input,
    TOutputStream* output,
    int bufferSize)
{
    struct TWriteBufferTag { };
    TBlob buffer(TWriteBufferTag(), bufferSize);

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
