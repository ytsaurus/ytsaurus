#pragma once

#include "common.h"

#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase
    : public node::ObjectWrap
{
protected:
    TNodeJSStreamBase();
    ~TNodeJSStreamBase();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    struct TOutputPart
    {
        // The following data is allocated on the heap hence have to care
        // about ownership transfer and/or freeing memory after structure
        // disposal.
        char*  Buffer;
        size_t Length;
    };

    struct TInputPart
    {
        uv_work_t Request;
        TNodeJSStreamBase* Stream;
        v8::Persistent<v8::Value> Handle;

        // The following data is owned by handle hence no need to care about
        // freeing memory after structure disposal.
        char*  Buffer;
        size_t Offset;
        size_t Length;
    };

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&);
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
