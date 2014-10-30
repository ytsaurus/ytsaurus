#include "stream_base.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

TNodeJSStreamBase::TNodeJSStreamBase()
    : node::ObjectWrap()
    , AsyncRefCounter_(0)
{ }

TNodeJSStreamBase::~TNodeJSStreamBase()
{ }

void TNodeJSStreamBase::AsyncRef(bool acquireSyncRef)
{
    if (AsyncRefCounter_++ == 0) {
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
    auto oldRefCounter = AsyncRefCounter_--;
    YASSERT(oldRefCounter > 0);
    if (oldRefCounter == 1) {
        EIO_PUSH(TNodeJSStreamBase::UnrefCallback, this);
    }
}

int TNodeJSStreamBase::UnrefCallback(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSStreamBase* stream = static_cast<TNodeJSStreamBase*>(request->data);

    YASSERT(stream->AsyncRefCounter_.load() == 0);

    stream->Unref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
