#include "common.h"
#include "input_stream.h"
#include "output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

class TInputStreamStub
    : public node::ObjectWrap
{
protected:
    TInputStreamStub(TNodeJSInputStream* slave);
    ~TInputStreamStub();

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
    // XXX(sandello): Investigate whether it is safe to call Ref() and Unref() on an V8 object
    // from other threads.
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
        TInputStreamStub* Host;

        Persistent<Function> Callback;

        char*  Buffer;
        size_t Length;

        TReadRequest(TInputStreamStub* host, Handle<Integer> length, Handle<Function> callback)
            : Host(host)
            , Callback(Persistent<Function>::New(callback))
            , Length(length->Uint32Value())
        {
            THREAD_AFFINITY_IS_V8();

            if (Length) {
                Buffer = new char[Length];
            }

            YASSERT(Buffer || !Length);

            Host->Ref();
            Host->Slave->AsyncRef(true);
        }

        ~TReadRequest()
        {
            THREAD_AFFINITY_IS_V8();

            if (Buffer) {
                delete[] Buffer;
            }

            Callback.Dispose();
            Callback.Clear();

            Host->Slave->AsyncUnref();
            Host->Unref();
        }
    };

private:
    TInputStreamStub(const TInputStreamStub&);
    TInputStreamStub& operator=(const TInputStreamStub&);
};

////////////////////////////////////////////////////////////////////////////////

TInputStreamStub::TInputStreamStub(TNodeJSInputStream* slave)
    : node::ObjectWrap()
    , Slave(slave)
{
    THREAD_AFFINITY_IS_V8();
}

TInputStreamStub::~TInputStreamStub()
{
    THREAD_AFFINITY_IS_V8();
}

Handle<Value> TInputStreamStub::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);
    EXPECT_THAT_HAS_INSTANCE(args[0], TNodeJSInputStream);

    TInputStreamStub* host = new TInputStreamStub(
        ObjectWrap::Unwrap<TNodeJSInputStream>(args[0].As<Object>()));
    host->Wrap(args.This());
    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamStub::ReadSynchronously(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TInputStreamStub* host =
        ObjectWrap::Unwrap<TInputStreamStub>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    // Do the work.
    size_t length;
    TReadString* string;

    length = args[0]->Uint32Value();
    string = new TInputStreamStub::TReadString(length);
    length = host->Slave->Read(string->mutable_data(), length);
    string->mutable_length() = length;

    return scope.Close(String::NewExternal(string));
}

Handle<Value> TInputStreamStub::Read(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YASSERT(args.Length() == 2);
    EXPECT_THAT_IS(args[0], Uint32);
    EXPECT_THAT_IS(args[1], Function);

    // Do the work.
    TReadRequest* request = new TReadRequest(
        ObjectWrap::Unwrap<TInputStreamStub>(args.This()),
        args[0].As<Integer>(),
        args[1].As<Function>());

    uv_queue_work(
        uv_default_loop(), &request->Request,
        TInputStreamStub::ReadWork, TInputStreamStub::ReadAfter);

    return Undefined();
}

void TInputStreamStub::ReadWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();
    TReadRequest* request =
        container_of(workRequest, TReadRequest, Request);

    request->Length = request->Host->Slave->Read(request->Buffer, request->Length);
}

void TInputStreamStub::ReadAfter(uv_work_t* workRequest)
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

        if (block.HasCaught()) {
            node::FatalException(block);
        }
    }

    delete request;
}

////////////////////////////////////////////////////////////////////////////////

class TOutputStreamStub
    : public node::ObjectWrap
{
protected:
    TOutputStreamStub(TNodeJSOutputStream* slave);
    ~TOutputStreamStub();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    // Synchronous JS API.
    static Handle<Value> New(const Arguments& args);

    static Handle<Value> WriteSynchronously(const Arguments& args);
    static Handle<Value> Flush(const Arguments& args);
    static Handle<Value> Finish(const Arguments& args);

    // Asynchronous JS API.
    static Handle<Value> Write(const Arguments& args);
    static void WriteWork(uv_work_t* workRequest);
    static void WriteAfter(uv_work_t* workRequest);

private:
    // XXX(sandello): See comments for TInputStreamStub
    TNodeJSOutputStream* Slave;

    struct TWriteRequest
    {
        uv_work_t Request;
        TOutputStreamStub* Host;

        Persistent<Function> Callback;
        String::Utf8Value ValueToBeWritten;

        char*  Buffer;
        size_t Length;

        TWriteRequest(TOutputStreamStub* host, Handle<String> string, Handle<Function> callback)
            : Host(host)
            , Callback(Persistent<Function>::New(callback))
            , ValueToBeWritten(string)
        {
            THREAD_AFFINITY_IS_V8();

            Buffer = *ValueToBeWritten;
            Length = ValueToBeWritten.length();

            Host->Ref();
            Host->Slave->AsyncRef(true);
        }

        ~TWriteRequest()
        {
            THREAD_AFFINITY_IS_V8();

            Callback.Dispose();
            Callback.Clear();

            Host->Slave->AsyncUnref();
            Host->Unref();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

TOutputStreamStub::TOutputStreamStub(TNodeJSOutputStream* slave)
    : node::ObjectWrap()
    , Slave(slave)
{
    THREAD_AFFINITY_IS_V8();
}

TOutputStreamStub::~TOutputStreamStub()
{
    THREAD_AFFINITY_IS_V8();
}

Handle<Value> TOutputStreamStub::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);
    EXPECT_THAT_HAS_INSTANCE(args[0], TNodeJSOutputStream);

    TOutputStreamStub* host = new TOutputStreamStub(
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args[0].As<Object>()));
    host->Wrap(args.This());
    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::WriteSynchronously(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamStub* host =
        ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);
    EXPECT_THAT_IS(args[0], String);

    // Do the work.
    String::Utf8Value value(args[0]);
    host->Slave->Write(*value, value.length());

    return Undefined();
}

Handle<Value> TOutputStreamStub::Write(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YASSERT(args.Length() == 2);
    EXPECT_THAT_IS(args[0], String);
    EXPECT_THAT_IS(args[1], Function);

    // Do the work.
    TWriteRequest* request = new TWriteRequest(
        ObjectWrap::Unwrap<TOutputStreamStub>(args.This()),
        args[0].As<String>(),
        args[1].As<Function>());

    uv_queue_work(
        uv_default_loop(), &request->Request,
        TOutputStreamStub::WriteWork, TOutputStreamStub::WriteAfter);

    return Undefined();
}

void TOutputStreamStub::WriteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();
    TWriteRequest* request =
        container_of(workRequest, TWriteRequest, Request);

    request->Host->Slave->Write(request->Buffer, request->Length);
}

void TOutputStreamStub::WriteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TWriteRequest* request =
        container_of(workRequest, TWriteRequest, Request);

    {
        TryCatch block;

        request->Callback->Call(Context::GetCurrent()->Global(), 0, NULL);

        if (block.HasCaught()) {
            node::FatalException(block);
        }
    }

    delete request;
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::Flush(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamStub* host =
        ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    host->Slave->Flush();

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::Finish(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamStub* host =
        ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    host->Slave->Finish();

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

void ExportInputStreamStub(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TInputStreamStub::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Read", TInputStreamStub::Read);
    NODE_SET_PROTOTYPE_METHOD(tpl, "ReadSynchronously", TInputStreamStub::ReadSynchronously);

    tpl->SetClassName(String::NewSymbol("TInputStreamStub"));
    target->Set(String::NewSymbol("TInputStreamStub"), tpl->GetFunction());
}

void ExportOutputStreamStub(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TOutputStreamStub::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Write", TOutputStreamStub::Write);
    NODE_SET_PROTOTYPE_METHOD(tpl, "WriteSynchronously", TOutputStreamStub::WriteSynchronously);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Flush", TOutputStreamStub::Flush);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Finish", TOutputStreamStub::Finish);

    tpl->SetClassName(String::NewSymbol("TOutputStreamStub"));
    target->Set(String::NewSymbol("TOutputStreamStub"), tpl->GetFunction());
}

void ExportYTStubs(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ExportInputStreamStub(target);
    ExportOutputStreamStub(target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

NODE_MODULE(ytnode_stubs, NYT::ExportYTStubs)
