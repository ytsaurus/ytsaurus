#pragma once

#include "common.h"

#include <deque>

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

    struct TJSPart
    {
        uv_work_t Request;
        TNodeJSStreamBase* Stream;
        v8::Persistent<v8::Value> Handle;

        char*  Data;
        size_t Offset;
        size_t Length;
    };

    typedef std::deque<TJSPart*> TQueue;

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&);
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
