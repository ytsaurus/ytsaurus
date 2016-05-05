#pragma once

#include "stream_base.h"

#include <yt/core/concurrency/event_count.h>

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to TOutputStream interface as a C++ object and
//! simultaneously provides 'readable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from C++ to JS.
class TOutputStreamWrap
    : public TNodeJSStreamBase
    , public TOutputStream
{
protected:
    TOutputStreamWrap(ui64 lowWatermark, ui64 highWatermark);
    ~TOutputStreamWrap() throw();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Pull(const v8::Arguments& args);
    v8::Handle<v8::Value> DoPull();

    static v8::Handle<v8::Value> Drain(const v8::Arguments& args);
    void DoDrain();

    static v8::Handle<v8::Value> Destroy(const v8::Arguments& args);
    void DoDestroy();

    static v8::Handle<v8::Value> IsEmpty(const v8::Arguments& args);
    static v8::Handle<v8::Value> IsDestroyed(const v8::Arguments& args);
    static v8::Handle<v8::Value> IsPaused(const v8::Arguments& args);
    static v8::Handle<v8::Value> IsCompleted(const v8::Arguments& args);

    // Asynchronous JS API.
    static int AsyncOnData(eio_req* request);
    void EmitAndStifleOnData();
    void IgniteOnData();

    // Diagnostics.
    const ui32 GetBytesEnqueued()
    {
        return BytesEnqueued_.load(std::memory_order_relaxed);
    }

    const ui32 GetBytesDequeued()
    {
        return BytesDequeued_.load(std::memory_order_relaxed);
    }

    void SetCompleted();

protected:
    // C++ API.
    void DoWrite(const void* buffer, size_t length) override;
    void DoWriteV(const TPart* parts, size_t count) override;

private:
    void WritePrologue();
    void WriteEpilogue(char* buffer, size_t length);

    void DisposeBuffers();

private:
    std::atomic<bool> IsDestroyed_ = {false};
    std::atomic<bool> IsPaused_ = {false};
    std::atomic<bool> IsCompleted_ = {false};

    std::atomic<ui64> BytesInFlight_ = {0};
    std::atomic<ui64> BytesEnqueued_ = {0};
    std::atomic<ui64> BytesDequeued_ = {0};

    const ui64 LowWatermark_;
    const ui64 HighWatermark_;

    NConcurrency::TEventCount Conditional_;
    TLockFreeQueue<TOutputPart> Queue_;

private:
    TOutputStreamWrap(const TOutputStreamWrap&);
    TOutputStreamWrap& operator=(const TOutputStreamWrap&);
};

inline void TOutputStreamWrap::EmitAndStifleOnData()
{
    bool expected = false;
    if (IsPaused_.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        AsyncRef(false);
        EIO_PUSH(TOutputStreamWrap::AsyncOnData, this);
    }
}

inline void TOutputStreamWrap::IgniteOnData()
{
    bool expected = true;
    if (IsPaused_.compare_exchange_strong(expected, false, std::memory_order_release)) {
        if (!Queue_.IsEmpty()) {
            EmitAndStifleOnData();
        }
    }
}

inline void TOutputStreamWrap::SetCompleted()
{
    IsCompleted_ = true;

    Conditional_.NotifyAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
