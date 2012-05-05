#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

#ifndef _GLIBCXX_PURE
#define _GLIBCXX_PURE inline
#endif

#include <deque>
#include <memory>

#define INPUT_QUEUE_SIZE 128

namespace NYT {

COMMON_V8_USES

using v8::Context;
using v8::Exception;
using v8::ThrowException;
using v8::TryCatch;

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Extract this method to a separate file.
void DoNothing(uv_work_t*)
{ }

////////////////////////////////////////////////////////////////////////////////

class TTestInputStream
    : public node::ObjectWrap
{
protected:
    TTestInputStream(TNodeJSInputStream* slave);
    ~TTestInputStream();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    // Synchronous JS API.
    static Handle<Value> New(const Arguments& args);

    static Handle<Value> ReadSynchronously(const Arguments& args);

    // Asynchronous JS API.
    static Handle<Value> Read(const Arguments& args);
    static void ReadWork(uv_work_t* workRequest);
    static void ReadAfter(uv_work_t* workRequest);

private:
    // XXX(sandello): Store by intrusive ptr afterwards.
    TNodeJSInputStream* Slave;

    class TReadString
        : public String::ExternalAsciiStringResource
    {
    public:
        TReadString(size_t requiredLength)
            : Buffer(new char[requiredLength])
            , Length(requiredLength)
        { }

        ~TReadString()
        {
            if (Buffer) {
                delete[] Buffer;
            }
        }

        const char* data() const
        {
            return Buffer;
        }

        char* mutable_data()
        {
            return Buffer;
        }

        size_t length() const
        {
            return Length;
        }

        size_t& mutable_length()
        {
            return Length;
        }

    private:
        char*  Buffer;
        size_t Length;           
    };

    struct TReadRequest
    {
        uv_work_t Request;
        TTestInputStream* Stream;

        Persistent<Function> Callback;

        char*  Buffer;
        size_t Length;
    };

private:
    TTestInputStream(const TTestInputStream&);
    TTestInputStream& operator=(const TTestInputStream&);
};

////////////////////////////////////////////////////////////////////////////////

TTestInputStream::TTestInputStream(TNodeJSInputStream* slave)
    : node::ObjectWrap()
    , Slave(slave)
{
    T_THREAD_AFFINITY_IS_V8();
}

TTestInputStream::~TTestInputStream()
{
    // Affinity: any?
    TRACE_CURRENT_THREAD("??");
}

Handle<Value> TTestInputStream::New(const Arguments& args)
{
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    assert(args.Length() == 1);

    TTestInputStream* host = new TTestInputStream(
        ObjectWrap::Unwrap<TNodeJSInputStream>(Local<Object>::Cast(args[0])));
    host->Wrap(args.This());
    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TTestInputStream::ReadSynchronously(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TTestInputStream* host =
        ObjectWrap::Unwrap<TTestInputStream>(args.This());

    // Validate arguments.
    assert(args.Length() == 1);

    if (!args[0]->IsUint32()) {
        return ThrowException(Exception::TypeError(
            String::New("Expected first argument to be an Uint32")));
    }

    // Do the work.
    size_t length;
    TReadString* string;

    length = args[0]->Uint32Value();
    string = new TTestInputStream::TReadString(length);
    length = host->Slave->Read(string->mutable_data(), length);
    string->mutable_length() = length;

    return scope.Close(String::NewExternal(string));
}

Handle<Value> TTestInputStream::Read(const Arguments &args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    assert(args.Length() == 2);

    if (!args[0]->IsUint32()) {
        return ThrowException(Exception::TypeError(
            String::New("Expected first argument to be an Uint32")));
    }
    if (!args[1]->IsFunction()) {
        return ThrowException(Exception::TypeError(
            String::New("Expected second argument to be a Function")));
    }

    // Do the work.
    TReadRequest* request = new TReadRequest();

    request->Callback = Persistent<Function>::New(Local<Function>::Cast(args[1])); 
    request->Stream = ObjectWrap::Unwrap<TTestInputStream>(args.This());
    request->Length = args[0]->Uint32Value();
    request->Buffer = new char[request->Length];

    uv_queue_work(
        uv_default_loop(), &request->Request,
        TTestInputStream::ReadWork, TTestInputStream::ReadAfter);

    request->Stream->Ref();
    request->Stream->Slave->Ref();

    return Undefined();
}

void TTestInputStream::ReadWork(uv_work_t *workRequest)
{
    THREAD_AFFINITY_IS_UV();
    TReadRequest* request =
        container_of(workRequest, TReadRequest, Request);

    request->Length = request->Stream->Slave->Read(request->Buffer, request->Length);
}

void TTestInputStream::ReadAfter(uv_work_t *workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TReadRequest* request =
        container_of(workRequest, TReadRequest, Request);

    {
        TryCatch block;

        Local<Value> args[] = {
            Local<Value>::New(Integer::New(request->Length)),
            Local<Value>::New(String::New(request->Buffer, request->Length))
        };

        request->Callback->Call(Context::GetCurrent()->Global(), 2, args);
        request->Callback.Dispose();

        if (block.HasCaught()) {
            node::FatalException(block);
        }
    }

    request->Stream->Slave->Unref();
    request->Stream->Unref();

    delete[] request->Buffer;
    delete   request;
}

////////////////////////////////////////////////////////////////////////////////

void InitInputStream(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSInputStream::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Push", TNodeJSInputStream::Push);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Sweep", TNodeJSInputStream::Sweep);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Close", TNodeJSInputStream::Close);

    tpl->SetClassName(String::NewSymbol("TNodeJSInputStream"));
    target->Set(String::NewSymbol("TNodeJSInputStream"), tpl->GetFunction());
}

void InitTestInputStream(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TTestInputStream::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Read", TTestInputStream::Read);
    NODE_SET_PROTOTYPE_METHOD(tpl, "ReadSynchronously", TTestInputStream::ReadSynchronously);

    tpl->SetClassName(String::NewSymbol("TTestInputStream"));
    target->Set(String::NewSymbol("TTestInputStream"), tpl->GetFunction());
}

void InitYT(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    InitInputStream(target);
    InitTestInputStream(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(yt_test, NYT::InitYT)
