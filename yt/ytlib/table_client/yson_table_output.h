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
    TYsonTableOutput(ISyncWriter* syncWriter);

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    TIntrusivePtr<ISyncWriter> Writer;
    TAutoPtr<TRowConsumer> RowConsumer;
    NYTree::TYsonParser YsonParser;
};

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
