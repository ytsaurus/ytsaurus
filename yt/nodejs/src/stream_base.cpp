#include "stream_base.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

TNodeJSStreamBase::TNodeJSStreamBase() = default;

TNodeJSStreamBase::~TNodeJSStreamBase()
{
    YCHECK(AsyncRefCounter_ == 0);
    YCHECK(refs_ == 0);
}

void TNodeJSStreamBase::AsyncRef(bool acquireSyncRef)
{
    if (AsyncRefCounter_.fetch_add(1, std::memory_order_acquire) == 0) {
        if (acquireSyncRef) {
            THREAD_AFFINITY_IS_V8();
            Ref();
        } else {
            THREAD_AFFINITY_IS_ANY();
            YUNREACHABLE();
        }
    }
}

void TNodeJSStreamBase::AsyncUnref()
{
    auto oldRefCounter = AsyncRefCounter_.fetch_sub(1, std::memory_order_release);
    YASSERT(oldRefCounter > 0);
    if (oldRefCounter == 1) {
        EIO_PUSH(TNodeJSStreamBase::UnrefCallback, this);
    }
}

int TNodeJSStreamBase::UnrefCallback(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    auto* stream = static_cast<TNodeJSStreamBase*>(request->data);
    YCHECK(stream->AsyncRefCounter_ + stream->refs_ > 0);
    stream->Unref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
