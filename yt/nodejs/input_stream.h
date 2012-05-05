#pragma once

#include "stream_base.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to TInputStream interface as a C++ object and
//! simultaneously provides 'writable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from JS to C++.
class TNodeJSInputStream
    : public TNodeJSStreamBase
{
protected:
    TNodeJSInputStream();
    ~TNodeJSInputStream();

public:
    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Push(const v8::Arguments& args);
    void DoPush(v8::Handle<v8::Value> buffer, char *data, size_t offset, size_t length);

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Sweep(const v8::Arguments& args);
    static void AsyncSweep(uv_work_t *request);
    void EnqueueSweep();
    void DoSweep();

    static v8::Handle<v8::Value> Close(const v8::Arguments& args);
    static void AsyncClose(uv_work_t *request);
    void EnqueueClose();
    void DoClose();

    // C++ API.
    size_t Read(void* buffer, size_t length);

private:
    friend class TNodeJSDriverHost;
    
    bool IsAlive;
    pthread_mutex_t Mutex;
    pthread_cond_t Conditional;
    TQueue Queue;

    uv_work_t SweepRequest;
    uv_work_t CloseRequest;
};

inline void TNodeJSInputStream::EnqueueSweep()
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &SweepRequest,
        DoNothing, TNodeJSInputStream::AsyncSweep);
}

inline void TNodeJSInputStream::EnqueueClose()
{
    // Post to any worker thread.
    uv_queue_work(
        uv_default_loop(), &CloseRequest,
        TNodeJSInputStream::AsyncClose, DoNothing);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
