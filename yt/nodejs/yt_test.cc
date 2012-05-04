#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

#include <node.h>
#include <node_buffer.h>

#ifndef _GLIBCXX_PURE
#define _GLIBCXX_PURE inline
#endif

#include <deque>
#include <memory>

#define INPUT_QUEUE_SIZE 128

namespace NYT {

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

class TNodeJSStreamBase;

//! This class adheres to TInputStream interface as a C++ object and
//! simultaneously provides 'writable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from JS to C++.
class TNodeJSInputStream;

//! This class adheres to TOutputStream interface as a C++ object and
//! simultaneously provides 'readable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from C++ to JS.
class TNodeJSOutputStream;

//! A NodeJS driver host.
class TNodeJSDriverHost;

////////////////////////////////////////////////////////////////////////////////

void DoNothing(uv_work_t*)
{ }

////////////////////////////////////////////////////////////////////////////////

class TNodeJSDriverHost
    : public node::ObjectWrap
{
public:
    ~TNodeJSDriverHost();

    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    // Synchronous JS API.
    static Handle<Value> New(const Arguments& args);
    static Handle<Value> WriteToInputStream(const Arguments& args);
    static Handle<Value> ReadFromOutputStream(const Arguments& args);
    static Handle<Value> ReadFromErrorStream(const Arguments& args);
    static Handle<Value> CloseInputStream(const Arguments& args);
    static Handle<Value> CloseOutputStream(const Arguments& args);
    static Handle<Value> CloseErrorStream(const Arguments& args);

    // Asynchronous JS API.
    static Handle<Value> Test(const Arguments& args);
    static void TestWork(uv_work_t* workRequest);
    static void TestAfter(uv_work_t* workRequest);

private:
    std::shared_ptr<TNodeJSInputStream> InputStream;
//    std::list<> OutputStreamParts;
//    std::list<> ErrorStreamParts;

    uv_work_t TestRequest;

private:
    static TNodeJSDriverHost* Create();
    TNodeJSDriverHost();
    TNodeJSDriverHost(const TNodeJSDriverHost&);
};

Handle<Value> TNodeJSDriverHost::New(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TNodeJSDriverHost* host = new TNodeJSDriverHost();
    host->Wrap(args.This());
    return args.This();
}

Handle<Value> TNodeJSDriverHost::WriteToInputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TNodeJSDriverHost* host =
        ObjectWrap::Unwrap<TNodeJSDriverHost>(args.This());

    // Validate arguments.
    assert(args.Length() == 3);

    assert(node::Buffer::HasInstance(args[0]));
    assert(args[1]->IsUint32());
    assert(args[2]->IsUint32());

    // Do the work.
    host->InputStream->Push(
        /* buffer */ args[0],
        /* data   */ node::Buffer::Data(Local<Object>::Cast(args[0])),
        /* offset */ args[1]->Uint32Value(),
        /* length */ args[2]->Uint32Value());

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}

Handle<Value> TNodeJSDriverHost::ReadFromOutputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

Handle<Value> TNodeJSDriverHost::ReadFromErrorStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

Handle<Value> TNodeJSDriverHost::CloseInputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    TNodeJSDriverHost* host =
        ObjectWrap::Unwrap<TNodeJSDriverHost>(args.This());

    host->InputStream->AsyncClose();
    return Undefined();
}

Handle<Value> TNodeJSDriverHost::CloseOutputStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

Handle<Value> TNodeJSDriverHost::CloseErrorStream(const Arguments& args)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    return Undefined();
}

struct TTestRequest
{
    uv_work_t TestRequest;
    TNodeJSDriverHost* Host;
    size_t HowMuch;
    char* WhereToRead;
};

Handle<Value> TNodeJSDriverHost::Test(const Arguments &args)
{
    //TRACE_CURRENT_THREAD();
    assert(args.Length() == 1);
    assert(args[0]->IsUint32());

    TTestRequest* request = new TTestRequest();

    request->Host = ObjectWrap::Unwrap<TNodeJSDriverHost>(args.This());
    request->HowMuch = args[0]->ToUint32()->Value();
    request->WhereToRead = new char[request->HowMuch];

    uv_queue_work(
        uv_default_loop(), &request->TestRequest,
        TNodeJSDriverHost::TestWork, TNodeJSDriverHost::TestAfter);

    request->Host->Ref();
    return Undefined();
}

void TNodeJSDriverHost::TestWork(uv_work_t *workRequest)
{
    //TRACE_CURRENT_THREAD();
    TTestRequest* request = container_of(workRequest, TTestRequest, TestRequest);

    size_t x = request->Host->InputStream->Read(request->WhereToRead, request->HowMuch);
    request->HowMuch = x;
}

void TNodeJSDriverHost::TestAfter(uv_work_t *workRequest)
{
    //TRACE_CURRENT_THREAD();
    TTestRequest* request = container_of(workRequest, TTestRequest, TestRequest);

    fprintf(stderr, ">>>READ>>>");
    fwrite(request->WhereToRead, 1, request->HowMuch, stderr);
    fprintf(stderr, "<<< (len=%lu)\n", request->HowMuch);

    request->Host->Unref();

    delete[] request->WhereToRead;
    delete request;
}

TNodeJSDriverHost* TNodeJSDriverHost::Create()
{
    TRACE_CURRENT_THREAD();
    return new TNodeJSDriverHost();
}

TNodeJSDriverHost::TNodeJSDriverHost()
    : node::ObjectWrap()
    , InputStream(new TNodeJSInputStream())
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
}

TNodeJSDriverHost::~TNodeJSDriverHost()
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
}

////////////////////////////////////////////////////////////////////////////////

void InitYT(Handle<Object> target)
{
    // Affinity: V8
    TRACE_CURRENT_THREAD();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSDriverHost::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "write_to_input", TNodeJSDriverHost::WriteToInputStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "read_from_output", TNodeJSDriverHost::ReadFromOutputStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "read_from_error", TNodeJSDriverHost::ReadFromErrorStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "close_input", TNodeJSDriverHost::CloseInputStream);
    NODE_SET_PROTOTYPE_METHOD(tpl, "test", TNodeJSDriverHost::Test);

    tpl->SetClassName(String::NewSymbol("DriverHost"));
    target->Set(String::NewSymbol("DriverHost"), tpl->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(yt_test, NYT::InitYT)
