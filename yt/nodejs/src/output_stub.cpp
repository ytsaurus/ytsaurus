#include "output_stub.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TWriteRequest
{
    uv_work_t Request;
    std::shared_ptr<TNodeJSOutputStack> Stack;

    Persistent<Function> Callback;
    String::Utf8Value ValueToBeWritten;

    char*  Buffer;
    size_t Length;

    TWriteRequest(
        const std::shared_ptr<TNodeJSOutputStack>& stack,
        Handle<String> string,
        Handle<Function> callback)
        : Stack(stack)
        , Callback(Persistent<Function>::New(callback))
        , ValueToBeWritten(string)
    {
        THREAD_AFFINITY_IS_V8();

        Buffer = *ValueToBeWritten;
        Length = ValueToBeWritten.length();
    }

    ~TWriteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        Callback.Dispose();
        Callback.Clear();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TOutputStreamStub::ConstructorTemplate;

TOutputStreamStub::TOutputStreamStub()
    : node::ObjectWrap()
{
    THREAD_AFFINITY_IS_V8();
}

TOutputStreamStub::~TOutputStreamStub()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStreamStub::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TOutputStreamStub::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TOutputStreamStub"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Reset", TOutputStreamStub::Reset);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "AddCompression", TOutputStreamStub::AddCompression);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Write", TOutputStreamStub::Write);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "WriteSynchronously", TOutputStreamStub::WriteSynchronously);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Flush", TOutputStreamStub::Flush);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Finish", TOutputStreamStub::Finish);

    target->Set(
        String::NewSymbol("TOutputStreamStub"),
        ConstructorTemplate->GetFunction());
}

bool TOutputStreamStub::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 0);

    auto* host = new TOutputStreamStub();
    host->Wrap(args.This());
    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::Reset(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Do the work.
    switch (args.Length()) {
        case 0:
            host->Stack = NULL;
            break;

        case 1:
            EXPECT_THAT_HAS_INSTANCE(args[0], TOutputStreamWrap);
            auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args[0].As<Object>());
            host->Stack = std::make_shared<TNodeJSOutputStack>(stream);
            break;
    }

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::AddCompression(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    // Do the work.
    ECompression compression = static_cast<ECompression>(args[0]->Uint32Value());
    host->Stack->AddCompression(compression);

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::WriteSynchronously(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 1);
    EXPECT_THAT_IS(args[0], String);

    // Do the work.
    String::Utf8Value value(args[0]);
    host->Stack->Write(*value, value.length());

    return Undefined();
}

Handle<Value> TOutputStreamStub::Write(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YCHECK(args.Length() == 2);
    EXPECT_THAT_IS(args[0], String);
    EXPECT_THAT_IS(args[1], Function);

    // Do the work.
    TWriteRequest* request = new TWriteRequest(
        ObjectWrap::Unwrap<TOutputStreamStub>(args.This())->Stack,
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

    request->Stack->Write(request->Buffer, request->Length);
}

void TOutputStreamStub::WriteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TWriteRequest* request = container_of(workRequest, TWriteRequest, Request);

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
    auto* host = ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    host->Stack->Flush();

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamStub::Finish(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TOutputStreamStub>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    host->Stack->Finish();

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
