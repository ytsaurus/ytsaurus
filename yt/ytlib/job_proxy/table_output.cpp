#include "stdafx.h"
#include "table_output.h"

#include <ytlib/table_client/sync_writer.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(NTableClient::ISyncWriterPtr writer)
    : Writer(writer)
    , Consumer(writer)
    , YsonParser(&Consumer, NYTree::EYsonType::ListFragment)
{
    Writer->Open();
}

TTableOutput::~TTableOutput() throw()
{ }

void TTableOutput::DoWrite(const void* buf, size_t len)
{
    YsonParser.Consume(TStringBuf(static_cast<const char*>(buf), len));
}

void TTableOutput::DoFinish()
{
    YsonParser.Finish();
    Writer->Close();
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
