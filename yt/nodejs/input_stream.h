#pragma once

#include "stream_base.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSInputStream
    : public TNodeJSStreamBase
{
public:
    TNodeJSInputStream();
    ~TNodeJSInputStream();

public:
    // Synchronous JS API.
    void Push(v8::Handle<v8::Value> buffer, char *data, size_t offset, size_t length);

    // Asynchronous JS API.
    void AsyncSweep();
    static void Sweep(uv_work_t *request);
    void DoSweep();

    void AsyncClose();
    static void Close(uv_work_t *request);
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

inline void TNodeJSInputStream::AsyncSweep()
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &SweepRequest,
        DoNothing, TNodeJSInputStream::Sweep);
}

inline void TNodeJSInputStream::AsyncClose()
{
    // Post to any thread.
    uv_queue_work(
        uv_default_loop(), &CloseRequest,
        TNodeJSInputStream::Close, DoNothing);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
