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

void ReadToConsumer(
    const TFormat& format, 
    TWritingTableConsumer* consumer, 
    TInputStream* input,
    int readBufferSize)
{
    auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer);

    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(readBufferSize);

    while (true) {
        size_t length = input->Read(buffer.Begin(), buffer.Size());
        if (length == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), length));
        consumer->Flush();
    }

    // Dump everything into consumer.
    parser->Finish();
    // Flush everything into writer.
    consumer->Flush();
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NVersionedTableClient
