#pragma once

#include "common.h"
#include "output_stack.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TOutputStreamStub
    : public node::ObjectWrap
{
protected:
    TOutputStreamStub();
    ~TOutputStreamStub();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);
    static v8::Handle<v8::Value> Reset(const v8::Arguments& args);

    static v8::Handle<v8::Value> AddCompression(const v8::Arguments& args);

    static v8::Handle<v8::Value> WriteSynchronously(const v8::Arguments& args);

    static v8::Handle<v8::Value> Flush(const v8::Arguments& args);
    static v8::Handle<v8::Value> Finish(const v8::Arguments& args);

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Write(const v8::Arguments& args);
    static void WriteWork(uv_work_t* workRequest);
    static void WriteAfter(uv_work_t* workRequest);

private:
    TSharedPtr<TNodeJSOutputStack> Stack;

private:
    TOutputStreamStub(const TOutputStreamStub&);
    TOutputStreamStub& operator=(const TOutputStreamStub&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
