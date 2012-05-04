#pragma once

#include "common.h"

#include <deque>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase
{
public:
    TNodeJSStreamBase();
    virtual ~TNodeJSStreamBase();

    struct TPart
    {
        v8::Persistent<v8::Value> Buffer;

        char*  Data;
        size_t Offset;
        size_t Length;

        TPart()
            : Buffer()
            , Data(NULL)
            , Offset(-1)
            , Length(-1)
        { }
    };

    typedef std::deque<TPart> TQueue;

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&);
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
