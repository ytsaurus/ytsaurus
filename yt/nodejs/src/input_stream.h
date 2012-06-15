#pragma once

#include "stream_base.h"

#include <deque>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to TInputStream interface as a C++ object and
//! simultaneously provides 'writable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from JS to C++.
class TNodeJSInputStream
    : public TNodeJSStreamBase
    , public TInputStream
{
protected:
    TNodeJSInputStream(ui64 lowWatermark, ui64 highWatermark);
    ~TNodeJSInputStream() throw();

public:
    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Push(const v8::Arguments& args);
    v8::Handle<v8::Value> DoPush(v8::Persistent<v8::Value> handle, char* data, size_t offset, size_t length);

    static v8::Handle<v8::Value> End(const v8::Arguments& args);
    v8::Handle<v8::Value> DoEnd();

    static v8::Handle<v8::Value> Destroy(const v8::Arguments& args);
    v8::Handle<v8::Value> DoDestroy();

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Sweep(const v8::Arguments& args);
    static void AsyncSweep(uv_work_t* request);
    void EnqueueSweep();
    void DoSweep();

    static v8::Handle<v8::Value> Drain(const v8::Arguments& args);
    static void AsyncDrain(uv_work_t* request);
    void EnqueueDrain();
    void DoDrain();

protected:
    // C++ API.
    size_t DoRead(void* data, size_t length);

private:
    void DisposeHandles(std::deque<TInputPart*>* queue);
    void UpdateV8Properties();

private:
    TAtomic IsPushable;
    TAtomic IsReadable;

    TAtomic CurrentBufferSize;
    const ui64 LowWatermark;
    const ui64 HighWatermark;

    TMutex Mutex;
    TCondVar Conditional;
    std::deque<TInputPart*> ActiveQueue;
    std::deque<TInputPart*> InactiveQueue;

    uv_work_t SweepRequest;
    uv_work_t DrainRequest;

private:
    TNodeJSInputStream(const TNodeJSInputStream&);
    TNodeJSInputStream& operator=(const TNodeJSInputStream&);
};

inline void TNodeJSInputStream::EnqueueSweep()
{
    AsyncRef(false);
    SweepRequest.data = this;
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &SweepRequest,
        DoNothing, TNodeJSInputStream::AsyncSweep);
}

inline void TNodeJSInputStream::EnqueueDrain()
{
    AsyncRef(false);
    // Post to V8 thread.
    DrainRequest.data = this;
    uv_queue_work(
        uv_default_loop(), &DrainRequest,
        DoNothing, TNodeJSInputStream::AsyncDrain);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
