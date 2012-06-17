#include "stream_base.h"

namespace NYT {

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

TNodeJSStreamBase::TNodeJSStreamBase()
    : node::ObjectWrap()
    , AsyncRefCounter(0)
{
    uuid_generate_random(Uuid);
}

TNodeJSStreamBase::~TNodeJSStreamBase()
{ }

void TNodeJSStreamBase::AsyncRef(bool acquireSyncRef)
{
    if (NDetail::AtomicallyIncrement(&AsyncRefCounter) == 0) {
        if (acquireSyncRef) {
            THREAD_AFFINITY_IS_V8();
            Ref();
        }
    }
}

void TNodeJSStreamBase::AsyncUnref()
{
    YASSERT(NDetail::AtomicallyFetch(&AsyncRefCounter) >  0);
    if (NDetail::AtomicallyDecrement(&AsyncRefCounter) == 1) {
        EIO_NOP(TNodeJSStreamBase::UnrefCallback, this);
    }
}

void TNodeJSStreamBase::PrintUuid(char* buffer)
{
    uuid_unparse(Uuid, buffer);
}

int TNodeJSStreamBase::UnrefCallback(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSStreamBase* stream = static_cast<TNodeJSStreamBase*>(request->data);

    stream->Unref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
