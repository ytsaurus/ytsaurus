#pragma once

#include "public.h"

#include <ytlib/table_client/public.h>

#include <ytlib/formats/format.h>

namespace NYT {
namespace NVersionedTableClient {

//////////////////////////////////////////////////////////////////////////////////

void ReadToWriter(ISchemalessReaderPtr reader, ISchemalessWriterPtr writer, int rowBufferSize);

void ReadToConsumer(
    const NFormats::TFormat& format,
    NTableClient::TWritingTableConsumer* consumer,
    TInputStream* input,
    int readBufferSize);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NVersionedTableClient
