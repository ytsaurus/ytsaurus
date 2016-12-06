#pragma once

#include "common.h"
#include "input_stack.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamStub
    : public node::ObjectWrap
    , public TRefTracked<TInputStreamStub>
{
protected:
    TInputStreamStub();
    ~TInputStreamStub();

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

    static v8::Handle<v8::Value> ReadSynchronously(const v8::Arguments& args);

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Read(const v8::Arguments& args);
    static void ReadWork(uv_work_t* workRequest);
    static void ReadAfter(uv_work_t* workRequest);

private:
    TInputStreamStub(const TInputStreamStub&) = delete;
    TInputStreamStub& operator=(const TInputStreamStub&) = delete;

    TNodeJSInputStackPtr Stack;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
