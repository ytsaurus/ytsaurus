#pragma once

#include "stream_base.h"
#include "event_count.h"

#include <ytlib/misc/event_count.h>

#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to TOutputStream interface as a C++ object and
//! simultaneously provides 'readable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from C++ to JS.
class TNodeJSOutputStream
    : public TNodeJSStreamBase
    , public TOutputStream
{
protected:
    TNodeJSOutputStream(ui64 lowWatermark, ui64 highWatermark);
    ~TNodeJSOutputStream() throw();

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
    static v8::Handle<v8::Value> IsPaused(const v8::Arguments& args);
    static v8::Handle<v8::Value> IsDestroyed(const v8::Arguments& args);

    // Asynchronous JS API.
    static int AsyncOnData(eio_req* request);
    void EmitAndStifleOnData();
    void IgniteOnData();

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
    void DoWrite(const void* buffer, size_t length) override;

private:
    void DisposeBuffers();

private:
    TAtomic IsPaused_;
    TAtomic IsDestroyed_;

    TAtomic BytesInFlight;
    TAtomic BytesEnqueued;
    TAtomic BytesDequeued;
    
    const ui64 LowWatermark;
    const ui64 HighWatermark;

    TEventCount Conditional;
    TLockFreeQueue<TOutputPart> Queue;

private:
    TNodeJSOutputStream(const TNodeJSOutputStream&);
    TNodeJSOutputStream& operator=(const TNodeJSOutputStream&);
};

inline void TNodeJSOutputStream::EmitAndStifleOnData()
{
    if (AtomicCas(&IsPaused_, 1, 0)) {
        // Post to V8 thread.
        AsyncRef(false);
        EIO_PUSH(TNodeJSOutputStream::AsyncOnData, this);
    }
}

inline void TNodeJSOutputStream::IgniteOnData()
{
    if (AtomicCas(&IsPaused_, 0, 1)) {
        if (!Queue.IsEmpty()) {
            EmitAndStifleOnData();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
