#pragma once

#include "public.h"

#include <ytlib/table_client/public.h>

#include <ytlib/formats/format.h>

namespace NYT {
namespace NVersionedTableClient {

//////////////////////////////////////////////////////////////////////////////////

class TTableOutput
    : public TOutputStream
{
public:
    TTableOutput(const NFormats::TFormat& format, NTableClient::TWritingTableConsumer* consumer);
    ~TTableOutput() throw();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();


    NTableClient::TWritingTableConsumer* Consumer_;
    std::unique_ptr<NFormats::IParser> Parser_;
    bool IsParserValid_;

};

//////////////////////////////////////////////////////////////////////////////////

void ReadToWriter(ISchemalessReaderPtr reader, ISchemalessWriterPtr writer, int rowBufferSize);

void ReadToOutputStream(
    TOutputStream* output,
    TInputStream* input,
    int readBufferSize);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NVersionedTableClient
