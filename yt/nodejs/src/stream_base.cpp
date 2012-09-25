#include "stream_base.h"

namespace NYT {

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

TNodeJSStreamBase::TNodeJSStreamBase()
    : node::ObjectWrap()
    , AsyncRefCounter(0)
{ }

TNodeJSStreamBase::~TNodeJSStreamBase()
{ }

void TNodeJSStreamBase::AsyncRef(bool acquireSyncRef)
{
    if (NDetail::AtomicallyIncrement(&AsyncRefCounter) == 0) {
        if (acquireSyncRef) {
            THREAD_AFFINITY_IS_V8();
            Ref();
        } else {
            YUNREACHABLE();
        }
    }
}

void TNodeJSStreamBase::AsyncUnref()
{
    YASSERT(NDetail::AtomicallyFetch(&AsyncRefCounter) >  0);
    if (NDetail::AtomicallyDecrement(&AsyncRefCounter) == 1) {
        EIO_PUSH(TNodeJSStreamBase::UnrefCallback, this);
    }
}

int TNodeJSStreamBase::UnrefCallback(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSStreamBase* stream = static_cast<TNodeJSStreamBase*>(request->data);

    YASSERT(NDetail::AtomicallyFetch(&stream->AsyncRefCounter) == 0);

    stream->Unref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
