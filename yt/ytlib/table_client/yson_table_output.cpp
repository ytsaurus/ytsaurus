#include "stdafx.h"

#include "yson_table_output.h"
#include "yson_row_consumer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////

TYsonTableOutput::TYsonTableOutput(ISyncTableWriter* syncWriter)
    : Writer(syncWriter)
    , RowConsumer(new TRowConsumer(syncWriter))
    , YsonParser(RowConsumer.Get(), NYTree::EYsonType::ListFragment)
{
    Writer->Open();
}

TYsonTableOutput::~TYsonTableOutput() throw()
{ }

void TYsonTableOutput::DoWrite(const void* buf, size_t len)
{
    YsonParser.Consume(TStringBuf(static_cast<const char*>(buf), len));
}

void TYsonTableOutput::DoFinish()
{
    YsonParser.Finish();
    Writer->Close();
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
