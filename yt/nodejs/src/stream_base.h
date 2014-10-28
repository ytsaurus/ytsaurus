#pragma once

#include "common.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NNodeJS {

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

    void AsyncRef(bool acquireSyncRef);
    void AsyncUnref();

protected:
    std::atomic<NDetail::TNonVolatileCounter> AsyncRefCounter;

protected:
    struct TOutputPart
    {
        // The following data is allocated on the heap so we have to care
        // about ownership transfer and/or freeing memory after structure
        // disposal.
        char*  Buffer;
        size_t Length;
    };

    struct TInputPart
    {
        TNodeJSStreamBase* Stream;
        v8::Persistent<v8::Value> Handle;

        // The following data is owned by the handle hence no need to care
        // about freeing memory after structure disposal.
        char*  Buffer;
        size_t Offset;
        size_t Length;
    };

    template <bool acquireSyncRef>
    class TScopedRef
    {
        TNodeJSStreamBase* Stream;
    public:
        TScopedRef(TNodeJSStreamBase* stream)
            : Stream(stream)
        {
            Stream->AsyncRef(acquireSyncRef);
        }
        ~TScopedRef()
        {
            Stream->AsyncUnref();
        }
    };

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&);
    TNodeJSStreamBase(TNodeJSStreamBase&&);
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&);
    TNodeJSStreamBase& operator=(TNodeJSStreamBase&&);

    static int UnrefCallback(eio_req*);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
