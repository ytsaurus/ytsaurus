#include "async_ref_counted.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

IAsyncRefCounted::IAsyncRefCounted() = default;

IAsyncRefCounted::~IAsyncRefCounted()
{
    YCHECK(AsyncRefCounter_ == 0);
}

void IAsyncRefCounted::AsyncRef()
{
    if (AsyncRefCounter_.fetch_add(1, std::memory_order_acquire) == 0) {
        SyncRef();
    }
}

void IAsyncRefCounted::AsyncUnref()
{
    auto oldRefCounter = AsyncRefCounter_.fetch_sub(1, std::memory_order_release);
    YCHECK(oldRefCounter > 0);
    if (oldRefCounter == 1) {
        SyncUnref();
    }
}

int IAsyncRefCounted::GetAsyncRefCount() const
{
    return AsyncRefCounter_.load(std::memory_order_relaxed);
}

void Ref(IAsyncRefCounted* object)
{
    object->AsyncRef();
}

void Unref(IAsyncRefCounted* object)
{
    object->AsyncUnref();
}

////////////////////////////////////////////////////////////////////////////////

TAsyncRefCountedObjectWrap::TAsyncRefCountedObjectWrap()
    : node::ObjectWrap()
    , IAsyncRefCounted()
    , HomeThread_(pthread_self())
{
    THREAD_AFFINITY_IS_V8();
}

TAsyncRefCountedObjectWrap::~TAsyncRefCountedObjectWrap()
{
    THREAD_AFFINITY_IS_V8();

    YCHECK(IsHomeThread());

    YCHECK(refs_ == 0);
    YCHECK(GetAsyncRefCount() == 0);
}

void TAsyncRefCountedObjectWrap::SyncRef()
{
    YCHECK(IsHomeThread());
    node::ObjectWrap::Ref();
}

void TAsyncRefCountedObjectWrap::SyncUnref()
{
    if (IsHomeThread()) {
        node::ObjectWrap::Unref();
    } else {
        EIO_PUSH(TAsyncRefCountedObjectWrap::DelayedSyncUnref, this);
    }
}

bool TAsyncRefCountedObjectWrap::IsHomeThread() const
{
    return pthread_equal(pthread_self(), HomeThread_);
}

int TAsyncRefCountedObjectWrap::DelayedSyncUnref(eio_req* request)
{
    auto* wrap = static_cast<TAsyncRefCountedObjectWrap*>(request->data);
    YCHECK(wrap->IsHomeThread());
    YCHECK(wrap->refs_ + wrap->GetAsyncRefCount() > 0);
    wrap->SyncUnref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
