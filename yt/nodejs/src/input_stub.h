#pragma once

#include "common.h"
#include "input_stack.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamStub
    : public node::ObjectWrap
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
    TSharedPtr<TNodeJSInputStack> Stack;

private:
    TInputStreamStub(const TInputStreamStub&);
    TInputStreamStub& operator=(const TInputStreamStub&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
