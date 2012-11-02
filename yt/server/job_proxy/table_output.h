#pragma once

#include <ytlib/yson/public.h>
#include <ytlib/formats/public.h>
#include <ytlib/table_client/public.h>
#include <util/stream/output.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TTableOutput
    : public TOutputStream
{
public:
    TTableOutput(
        TAutoPtr<NFormats::IParser> parser,
        TAutoPtr<NYson::IYsonConsumer> consumer,
        const NTableClient::ISyncWriterPtr& writer);

    ~TTableOutput() throw();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    TAutoPtr<NFormats::IParser> Parser;

    // Just holds the consumer that parser is using.
    TAutoPtr<NYson::IYsonConsumer> Consumer;
    NTableClient::ISyncWriterPtr SyncWriter;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
