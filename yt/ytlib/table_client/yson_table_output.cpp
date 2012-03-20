#include "stdafx.h"

#include "yson_table_output.h"
#include "yson_row_consumer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////

TYsonTableOutput::TYsonTableOutput(ISyncWriter* syncWriter)
    : Writer(syncWriter)
    , RowConsumer(new TRowConsumer(syncWriter))
    , YsonParser(RowConsumer.Get(), true)
{
    Writer->Open();
}

void TYsonTableOutput::DoWrite(const void* buf, size_t len)
{
    const char* begin = static_cast<const char*>(buf);
    const char* end = begin + len;

    while(begin < end) {
        YsonParser.Consume(*begin);
        ++begin;
    }
}

void TYsonTableOutput::DoFinish()
{
    Writer->Close();
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
