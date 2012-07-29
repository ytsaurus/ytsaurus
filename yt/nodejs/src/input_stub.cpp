#include "input_stub.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

namespace {

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
    TSharedPtr<TNodeJSInputStack> Stack;

    Persistent<Function> Callback;

    char*  Buffer;
    size_t Length;

    TReadRequest(
        const TSharedPtr<TNodeJSInputStack>& stack,
        Handle<Integer> length,
        Handle<Function> callback)
        : Stack(stack)
        , Callback(Persistent<Function>::New(callback))
        , Length(length->Uint32Value())
    {
        THREAD_AFFINITY_IS_V8();

        if (Length) {
            Buffer = new char[Length];
        }

        YASSERT(Buffer || !Length);
    }

    ~TReadRequest()
    {
        THREAD_AFFINITY_IS_V8();

        if (Buffer) {
            delete[] Buffer;
        }

        Callback.Dispose();
        Callback.Clear();
    }
};

} // namespace

Persistent<FunctionTemplate> TInputStreamStub::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////

TInputStreamStub::TInputStreamStub()
    : node::ObjectWrap()
{
    THREAD_AFFINITY_IS_V8();
}

TInputStreamStub::~TInputStreamStub()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TInputStreamStub::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TInputStreamStub::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TInputStreamStub"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Reset", TInputStreamStub::Reset);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "AddCompression", TInputStreamStub::AddCompression);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Read", TInputStreamStub::Read);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "ReadSynchronously", TInputStreamStub::ReadSynchronously);

    target->Set(
        String::NewSymbol("TInputStreamStub"),
        ConstructorTemplate->GetFunction());
}

bool TInputStreamStub::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamStub::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 0);

    auto* host = new TInputStreamStub();
    host->Wrap(args.This());
    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamStub::Reset(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TInputStreamStub>(args.This());

    // Do the work.
    switch (args.Length()) {
        case 0:
            host->Stack = NULL;
            break;

        case 1:
            EXPECT_THAT_HAS_INSTANCE(args[0], TNodeJSInputStream);
            auto* stream = ObjectWrap::Unwrap<TNodeJSInputStream>(args[0].As<Object>());
            host->Stack = new TNodeJSInputStack(stream);
            break;
    }

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamStub::AddCompression(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TInputStreamStub>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    // Do the work.
    ECompression compression = static_cast<ECompression>(args[0]->Uint32Value());
    host->Stack->AddCompression(compression);

    return v8::Null();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamStub::ReadSynchronously(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* host = ObjectWrap::Unwrap<TInputStreamStub>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    // Do the work.
    size_t length;
    TReadString* string;

    length = args[0]->Uint32Value();
    string = new TReadString(length);
    length = host->Stack->Read(string->mutable_data(), length);
    string->mutable_length() = length;

    return scope.Close(String::NewExternal(string));
}

////////////////////////////////////////////////////////////////////////////////

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
        ObjectWrap::Unwrap<TInputStreamStub>(args.This())->Stack,
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

    request->Length = request->Stack->Read(request->Buffer, request->Length);
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

} // namespace NYT
