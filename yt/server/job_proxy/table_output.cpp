#include "stdafx.h"
#include "table_output.h"

#include <ytlib/formats/parser.h>
#include <ytlib/yson/consumer.h>
#include <ytlib/table_client/sync_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(
    std::unique_ptr<IParser> parser,
    std::unique_ptr<NYson::IYsonConsumer> consumer,
    ISyncWriterPtr syncWriter)
    : Parser(std::move(parser))
    , Consumer(std::move(consumer))
    , SyncWriter(std::move(syncWriter))
    , IsParserValid(true)
{ }

TTableOutput::~TTableOutput() throw()
{ }

void TTableOutput::DoWrite(const void* buf, size_t len)
{
	YCHECK(IsParserValid);
	try {
    	Parser->Read(TStringBuf(static_cast<const char*>(buf), len));
	} catch (const std::exception& ex) {
		IsParserValid = false;
		throw;
	}
}

void TTableOutput::DoFinish()
{
	if (IsParserValid) {
    	Parser->Finish();
    }
    SyncWriter->Close();
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
