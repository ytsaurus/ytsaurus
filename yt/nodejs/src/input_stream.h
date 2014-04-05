#pragma once

#include "stream_base.h"

#include <deque>

#include <util/system/mutex.h>
#include <util/system/condvar.h>

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
        return BytesEnqueued;
    }

    const ui32 GetBytesDequeued()
    {
        return BytesDequeued;
    }

protected:
    // C++ API.
    size_t DoRead(void* data, size_t length) override;

private:
    void Dispose();
    void DisposeHandles(std::deque<TInputPart*>* queue);
    void UpdateV8Properties();

private:
    // XXX(sandello): I believe these atomics are subject to false sharing due
    // to in-memory locality. But whatever -- it is not a bottleneck.
    TAtomic IsPushable;
    TAtomic IsReadable;

    TAtomic SweepRequestPending;
    TAtomic DrainRequestPending;

    TAtomic BytesInFlight;
    TAtomic BytesEnqueued;
    TAtomic BytesDequeued;

    const ui64 LowWatermark;
    const ui64 HighWatermark;

    TMutex Mutex;
    TCondVar Conditional;

    std::deque<TInputPart*> ActiveQueue;
    std::deque<TInputPart*> InactiveQueue;

private:
    TInputStreamWrap(const TInputStreamWrap&);
    TInputStreamWrap& operator=(const TInputStreamWrap&);
};

inline void TInputStreamWrap::EnqueueSweep(bool withinV8)
{
    if (AtomicCas(&SweepRequestPending, 1, 0)) {
        AsyncRef(withinV8);
        EIO_PUSH(TInputStreamWrap::AsyncSweep, this);
    }
}

inline void TInputStreamWrap::EnqueueDrain(bool withinV8)
{
    if (AtomicCas(&DrainRequestPending, 1, 0)) {
        AsyncRef(withinV8);
        EIO_PUSH(TInputStreamWrap::AsyncDrain, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
