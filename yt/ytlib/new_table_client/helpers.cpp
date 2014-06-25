#include "stdafx.h"

#include "helpers.h"

#include "schemaless_reader.h"
#include "schemaless_writer.h"
#include "unversioned_row.h"

#include <ytlib/formats/parser.h>

#include <ytlib/table_client/table_consumer.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;

//////////////////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(const TFormat& format, TWritingTableConsumer* consumer)
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
        Consumer_->Flush();
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
        // Flush everything into writer.
        Consumer_->Flush();
    }
}

//////////////////////////////////////////////////////////////////////////////////

void ReadToWriter(ISchemalessReaderPtr reader, ISchemalessWriterPtr writer, int rowBufferSize)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(rowBufferSize);

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

void ReadToOutputStream(
    TOutputStream* output,
    TInputStream* input,
    int readBufferSize)
{
    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(readBufferSize);

    while (true) {
        size_t length = input->Read(buffer.Begin(), buffer.Size());
        if (length == 0)
            break;

        output->Write(buffer.Begin(), length);
    }

    output->Close();
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NVersionedTableClient
