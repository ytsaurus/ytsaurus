#pragma once

#include "stream_base.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to TOutputStream interface as a C++ object and
//! simultaneously provides 'readable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from C++ to JS.
class TNodeJSOutputStream
    : public TNodeJSStreamBase
{
protected:
    TNodeJSOutputStream();
    ~TNodeJSOutputStream();

public:
    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    // Asynchronous JS API.
    static void AsyncOnWrite(uv_work_t* request);
    void EnqueueOnWrite(TPart* part);
    void DoOnWrite(TPart* part);

    static void AsyncOnFlush(uv_work_t* request);
    void EnqueueOnFlush();
    void DoOnFlush();

    static void AsyncOnFinish(uv_work_t* request);
    void EnqueueOnFinish();
    void DoOnFinish();

    // C++ API.
    void Write(const void* buffer, size_t length);
    void Flush();
    void Finish();

private:
    uv_work_t FlushRequest;
    uv_work_t FinishRequest;
};

inline void TNodeJSOutputStream::EnqueueOnWrite(TPart* part)
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &part->Request,
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
