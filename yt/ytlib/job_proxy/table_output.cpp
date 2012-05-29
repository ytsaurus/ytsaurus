#include "stdafx.h"
#include "table_output.h"

#include <ytlib/ytree/parser.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/table_client/sync_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(
    TAutoPtr<IParser> parser, 
    TAutoPtr<IYsonConsumer> consumer,
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
