#pragma once

#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

namespace NYT {

struct IDriver;

////////////////////////////////////////////////////////////////////////////////

class TNodeJSDriver
    : public node::ObjectWrap
{
protected:
    TNodeJSDriver();
    ~TNodeJSDriver();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Execute(const v8::Arguments& args);
    static void ExecuteWork(uv_work_t* workRequest);
    static void ExecuteAfter(uv_work_t* workRequest);

private:
    // TODO(sandello): Store by smart pointer.
    // TODO(sandello): Initialized this at some moment.
    IDriver* Driver;

private:
    TNodeJSDriver(const TNodeJSDriver&);
    TNodeJSDriver& operator=(const TNodeJSDriver&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
