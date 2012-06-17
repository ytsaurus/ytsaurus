#pragma once

#include "common.h"

#include <uuid/uuid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase
    : public node::ObjectWrap
{
protected:
    TNodeJSStreamBase();
    ~TNodeJSStreamBase();

    NDetail::TVolatileCounter AsyncRefCounter;

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    void AsyncRef(bool acquireSyncRef);
    void AsyncUnref();

    void PrintUuid(char* buffer);

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

protected:
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

    uuid_t Uuid;

private:
    TNodeJSStreamBase(const TNodeJSStreamBase&);
    TNodeJSStreamBase& operator=(const TNodeJSStreamBase&);

    static int UnrefCallback(eio_req*);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
