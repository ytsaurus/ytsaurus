#pragma once

#include "common.h"
#include "output_stack.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TOutputStreamStub
    : public node::ObjectWrap
    , public TRefTracked<TOutputStreamStub>
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

    static v8::Handle<v8::Value> Close(const v8::Arguments& args);

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Write(const v8::Arguments& args);
    static void WriteWork(uv_work_t* workRequest);
    static void WriteAfter(uv_work_t* workRequest);

private:
    TOutputStreamStub(const TOutputStreamStub&) = delete;
    TOutputStreamStub& operator=(const TOutputStreamStub&) = delete;

    TNodeJSOutputStackPtr Stack;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
