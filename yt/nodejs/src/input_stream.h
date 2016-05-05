#pragma once

#include "stream_base.h"

#include <util/system/condvar.h>
#include <util/system/mutex.h>

#include <deque>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to TInputStream interface as a C++ object and
//! simultaneously provides 'writable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from JS to C++.
class TInputStreamWrap
    : public TNodeJSStreamBase
    , public TInputStream
{
protected:
    TInputStreamWrap(ui64 lowWatermark, ui64 highWatermark);
    ~TInputStreamWrap() throw();

public:
    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Push(const v8::Arguments& args);
    v8::Handle<v8::Value> DoPush(v8::Persistent<v8::Value> handle, char* data, size_t offset, size_t length);

    static v8::Handle<v8::Value> End(const v8::Arguments& args);
    void DoEnd();

    static v8::Handle<v8::Value> Destroy(const v8::Arguments& args);
    void DoDestroy();

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Sweep(const v8::Arguments& args);
    static int AsyncSweep(eio_req* request);
    void EnqueueSweep(bool withinV8);
    void DoSweep();

    static v8::Handle<v8::Value> Drain(const v8::Arguments& args);
    static int AsyncDrain(eio_req* request);
    void EnqueueDrain(bool withinV8);
    void DoDrain();

    // Diagnostics.
    const ui32 GetBytesEnqueued()
    {
        return BytesEnqueued_.load(std::memory_order_relaxed);
    }

    const ui32 GetBytesDequeued()
    {
        return BytesDequeued_.load(std::memory_order_relaxed);
    }

protected:
    // C++ API.
    size_t DoRead(void* data, size_t length) override;

private:
    void Dispose();
    void DisposeHandles(std::deque<std::unique_ptr<TInputPart>>* queue);
    void UpdateV8Properties();

private:
    std::atomic<bool> IsPushable_ = {true};
    std::atomic<bool> IsReadable_ = {true};

    std::atomic<bool> SweepRequestPending_ = {false};
    std::atomic<bool> DrainRequestPending_ = {false};

    std::atomic<ui64> BytesInFlight_ = {0};
    std::atomic<ui64> BytesEnqueued_ = {0};
    std::atomic<ui64> BytesDequeued_ = {0};

    const ui64 LowWatermark_;
    const ui64 HighWatermark_;

    TMutex Mutex_;
    TCondVar Conditional_;

    std::deque<std::unique_ptr<TInputPart>> ActiveQueue_;
    std::deque<std::unique_ptr<TInputPart>> InactiveQueue_;

private:
    TInputStreamWrap(const TInputStreamWrap&) = delete;
    TInputStreamWrap& operator=(const TInputStreamWrap&) = delete;
};

inline void TInputStreamWrap::EnqueueSweep(bool withinV8)
{
    bool expected = false;
    if (SweepRequestPending_.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        AsyncRef(withinV8);
        EIO_PUSH(TInputStreamWrap::AsyncSweep, this);
    }
}

inline void TInputStreamWrap::EnqueueDrain(bool withinV8)
{
    bool expected = false;
    if (DrainRequestPending_.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        AsyncRef(withinV8);
        EIO_PUSH(TInputStreamWrap::AsyncDrain, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
