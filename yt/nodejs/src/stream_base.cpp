#include "stream_base.h"

namespace NYT {

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
        }
    }
}

void TNodeJSStreamBase::AsyncUnref()
{
    YASSERT(NDetail::AtomicallyFetch(&AsyncRefCounter) > 0);
    if (NDetail::AtomicallyDecrement(&AsyncRefCounter) == 1) {
        UnrefRequest.data = this;

        uv_queue_work(
            uv_default_loop(), &UnrefRequest,
            DoNothing, UnrefCallback);
    }
}

void TNodeJSStreamBase::UnrefCallback(uv_work_t* request)
{
    THREAD_AFFINITY_IS_V8();

    TNodeJSStreamBase* stream = static_cast<TNodeJSStreamBase*>(request->data);
    TNodeJSStreamBase* streamAlternative = container_of(request, TNodeJSStreamBase, UnrefRequest);

    YASSERT(stream == streamAlternative);

    stream->Unref();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
