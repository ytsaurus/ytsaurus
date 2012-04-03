#pragma once

#include "sync_writer.h"

#include <ytlib/ytree/yson_parser.h>

#include <util/stream/output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////

class TRowConsumer;

class TYsonTableOutput
    : public TOutputStream
{
public:
    TYsonTableOutput(ISyncTableWriter* syncWriter);
    ~TYsonTableOutput() throw();

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    TIntrusivePtr<ISyncTableWriter> Writer;
    TAutoPtr<TRowConsumer> RowConsumer;
    NYTree::TYsonParser YsonParser;
};

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
