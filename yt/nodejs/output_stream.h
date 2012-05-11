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
    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    // Asynchronous JS API.
    static void AsyncOnWrite(uv_work_t* request);
    void EnqueueOnWrite(TPart* part);
    void DoOnWrite(TPart* part);

    // C++ API.
    void Write(const void* buffer, size_t length);
};

inline void TNodeJSOutputStream::EnqueueOnWrite(TPart* part)
{
    // Post to V8 thread.
    uv_queue_work(
        uv_default_loop(), &part->Request,
        DoNothing, TNodeJSOutputStream::AsyncOnWrite);
}

void ExportOutputStream(v8::Handle<v8::Object> target);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
