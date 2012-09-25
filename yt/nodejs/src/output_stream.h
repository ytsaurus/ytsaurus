#pragma once

#include "stream_base.h"

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
    TNodeJSOutputStream();
    ~TNodeJSOutputStream() throw();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Destroy(const v8::Arguments& args);
    v8::Handle<v8::Value> DoDestroy();

    static v8::Handle<v8::Value> IsEmpty(const v8::Arguments& args);
    v8::Handle<v8::Value> DoIsEmpty();

    // Asynchronous JS API.
    static int AsyncOnWrite(eio_req* request);
    void EnqueueOnWrite();
    void DoOnWrite();

protected:
    // C++ API.
    void DoWrite(const void* buffer, size_t length) override;

private:
    void DisposeBuffers();

private:
    TAtomic IsWritable;

    TAtomic WriteRequestPending;
    TAtomic FlushRequestPending;
    TAtomic FinishRequestPending;

    TLockFreeQueue<TOutputPart> Queue;

private:
    TNodeJSOutputStream(const TNodeJSOutputStream&);
    TNodeJSOutputStream& operator=(const TNodeJSOutputStream&);
};

inline void TNodeJSOutputStream::EnqueueOnWrite()
{
    if (AtomicCas(&WriteRequestPending, 1, 0)) {
        // Post to V8 thread.
        AsyncRef(false);
        EIO_PUSH(TNodeJSOutputStream::AsyncOnWrite, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
