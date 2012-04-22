#pragma once

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/ytree/yson_parser.h>

#include <util/stream/output.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TTableOutput
    : public TOutputStream
{
public:
    TTableOutput(NTableClient::ISyncWriterPtr writer);
    ~TTableOutput() throw();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    NTableClient::ISyncWriterPtr Writer;
    NTableClient::TTableConsumer Consumer;
    NYTree::TYsonParser YsonParser;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
