#pragma once

#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

#include <ytlib/driver/public.h>
#include <ytlib/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeJSDriver
    : public node::ObjectWrap
{
protected:
    TNodeJSDriver(const NYTree::TYson& configuration);
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
    NDriver::IDriverPtr Driver;
    Stroka Message;

private:
    TNodeJSDriver(const TNodeJSDriver&);
    TNodeJSDriver& operator=(const TNodeJSDriver&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
