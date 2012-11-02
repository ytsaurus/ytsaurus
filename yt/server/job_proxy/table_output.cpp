#include "stdafx.h"
#include "table_output.h"

#include <ytlib/formats/parser.h>
#include <ytlib/yson/yson_consumer.h>
#include <ytlib/table_client/sync_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(
    TAutoPtr<IParser> parser, 
    TAutoPtr<NYson::IYsonConsumer> consumer,
    const ISyncWriterPtr& syncWriter)
    : Parser(parser)
    , Consumer(consumer)
    , SyncWriter(syncWriter)
{ }

TTableOutput::~TTableOutput() throw()
{ }

void TTableOutput::DoWrite(const void* buf, size_t len)
{
    Parser->Read(TStringBuf(static_cast<const char*>(buf), len));
}

void TTableOutput::DoFinish()
{
    Parser->Finish();
    SyncWriter->Close();
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
