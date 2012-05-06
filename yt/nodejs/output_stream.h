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

    static v8::Handle<v8::Value> Pull(const v8::Arguments& args);
    v8::Handle<v8::Value> DoPull();

    // Asynchronous JS API.

    // C++ API.
    void Write(const void* buffer, size_t length);

private:
    friend class TNodeJSDriverHost;

    static void DeleteCallback(char* data, void* hint);

    pthread_mutex_t Mutex;
    TQueue Queue;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
