#pragma once

#include "stream_base.h"


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

    // Asynchronous JS API.
    static void AsyncOnWrite(uv_work_t* request);
    void EnqueueOnWrite();
    void DoOnWrite();

    static void AsyncOnFlush(uv_work_t* request);
    void EnqueueOnFlush();
    void DoOnFlush();

    static void AsyncOnFinish(uv_work_t* request);
    void EnqueueOnFinish();
    void DoOnFinish();

protected:
    // C++ API.
    void DoWrite(const void* buffer, size_t length);
    void DoFlush();
    void DoFinish();

private:
    uv_work_t WriteRequest;
    uv_work_t FlushRequest;
    uv_work_t FinishRequest;

    TLockFreeQueue<TOutputPart> Queue;

private:
    TNodeJSOutputStream(const TNodeJSOutputStream&);
    TNodeJSOutputStream& operator=(const TNodeJSOutputStream&);
};

inline void TNodeJSOutputStream::EnqueueOnWrite()
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &WriteRequest,
        DoNothing, TNodeJSOutputStream::AsyncOnWrite);
}

inline void TNodeJSOutputStream::EnqueueOnFlush()
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &FlushRequest,
        DoNothing, TNodeJSOutputStream::AsyncOnFlush);
}

inline void TNodeJSOutputStream::EnqueueOnFinish()
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &FinishRequest,
        DoNothing, TNodeJSOutputStream::AsyncOnFinish);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
