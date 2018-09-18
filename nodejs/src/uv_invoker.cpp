#include "uv_invoker.h"

#include <yt/core/actions/invoker.h>

#include <yt/core/misc/lazy_ptr.h>

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

class TUVInvoker
    : public IInvoker
{
public:
    explicit TUVInvoker(uv_loop_t* loop)
    {
        THREAD_AFFINITY_IS_UV();

        memset(&AsyncHandle_, 0, sizeof(AsyncHandle_));
        YCHECK(uv_async_init(loop, &AsyncHandle_, &TUVInvoker::Callback) == 0);
        AsyncHandle_.data = this;
    }

    ~TUVInvoker()
    {
        THREAD_AFFINITY_IS_UV();

        uv_close((uv_handle_t*)&AsyncHandle_, nullptr);
    }

    virtual void Invoke(TClosure callback) override
    {
        THREAD_AFFINITY_IS_ANY();

        Queue_.Enqueue(std::move(callback));

        YCHECK(uv_async_send(&AsyncHandle_) == 0);
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return NConcurrency::InvalidThreadId;
    }

    virtual bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return invoker.Get() == this;
    }
#endif

private:
    uv_async_t AsyncHandle_;

    TLockFreeQueue<TClosure> Queue_;

    static void Callback(uv_async_t* handle, int status)
    {
        THREAD_AFFINITY_IS_V8();
        HandleScope scope;

        YCHECK(status == 0);
        YCHECK(handle->data);

        reinterpret_cast<TUVInvoker*>(handle->data)->CallbackImpl();
    }

    void CallbackImpl()
    {
        TClosure action;
        while (Queue_.Dequeue(&action)) {
            action.Run();
            action.Reset();
        }
    }
};

// uv_default_loop() is a static singleton object, so it is safe to call
// function at the binding time.
static TLazyIntrusivePtr<IInvoker> DefaultUVInvoker(BIND([] () -> IInvokerPtr {
    return New<TUVInvoker>(uv_default_loop());
}));

IInvokerPtr GetUVInvoker()
{
    return DefaultUVInvoker.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
