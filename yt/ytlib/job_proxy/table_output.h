#pragma once

#include <ytlib/ytree/public.h>
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
        TAutoPtr<NYTree::IParser> parser,
        TAutoPtr<NYTree::IYsonConsumer> consumer,
        const NTableClient::ISyncWriterPtr& writer);

    ~TTableOutput() throw();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    TAutoPtr<NYTree::IParser> Parser;

    // Just holds the consumer that parser is using.
    TAutoPtr<NYTree::IYsonConsumer> Consumer;
    NTableClient::ISyncWriterPtr SyncWriter;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
