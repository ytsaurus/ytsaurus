#include "driver.h"

#include <ytlib/logging/log_manager.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/formats/format.h>

#include <util/stream/zlib.h>
#include <util/stream/lz.h>

#include <string>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NDriver;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const size_t StreamBufferSize = 32 * 1024;

DECLARE_ENUM(ECompression,
    (None)
    (Gzip)
    (Deflate)
    (LZO)
    (LZF)
    (Snappy)
);

template <class T, size_t N>
class TGrowingStreamStack
{
public:
    static_assert(N >= 1, "You have to provide a base stream to grow on.");

    TGrowingStreamStack(T* base)
        : Head(Stack + N - 1)
    {
        * Head = base;
    }

    ~TGrowingStreamStack()
    {
        T** end = Stack + N - 1;
        for (T** current = Head; current != end; ++current) {
            delete *current;
        }
    }

    template <class U>
    void Add()
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT((Head > Stack));
        T* layer = new U(Top());
        *--Head = layer;
    }

    template <class U, class A1>
    void Add(A1&& a1)
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        T* layer = new U(Top(), ForwardRV<A1>(a1));
        *--Head = layer;
    }

    template <class U, class A1, class A2>
    void Add(A1&& a1, A2&& a2)
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        T* layer = new U(Top(), ForwardRV<A1>(a1), ForwardRV<A2>(a2));
        *--Head = layer;
    }

    template <class U, class A1, class A2, class A3>
    void Add(A1&& a1, A2&& a2, A3&& a3)
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        T* layer = new U(Top(), ForwardRV<A1>(a1), ForwardRV<A2>(a2), ForwardRV<A3>(a3));
        *--Head = layer;
    }


    T* Top() const
    {
        return *(Head);
    }

    T* Bottom() const
    {
        return *(Stack + N - 1);
    }

    T* const* begin() const
    {
        return Head;
    }

    T* const* end() const
    {
        return Stack + N - 1;
    }

private:
    typedef T* TPtr;

    TPtr Stack[N];
    TPtr* Head;
};

// TODO(sandello): Refactor this huge mess.
struct TExecuteRequest
{
    uv_work_t Request;
    TNodeJSDriver* Host;

    TGrowingStreamStack<TInputStream, 2> InputStack;
    TGrowingStreamStack<TOutputStream, 3> OutputStack;

    Persistent<Function> Callback;

    Stroka Exception;

    TDriverRequest DriverRequest;
    TDriverResponse DriverResponse;

    TExecuteRequest(
        TNodeJSDriver* host,
        TNodeJSInputStream* inputStream,
        TNodeJSOutputStream* outputStream,
        Handle<Function> callback)
        : Host(host)
        , InputStack(inputStream)
        , OutputStack(outputStream)
        , Callback(Persistent<Function>::New(callback))
    {
        THREAD_AFFINITY_IS_V8();

        YASSERT(Host);
        YASSERT(inputStream == GetNodeJSInputStream());
        YASSERT(outputStream == GetNodeJSOutputStream());

        Host->Ref();
        GetNodeJSInputStream()->AsyncRef(true);
        GetNodeJSOutputStream()->AsyncRef(true);
    }

    ~TExecuteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        Callback.Dispose();
        Callback.Clear();

        GetNodeJSOutputStream()->AsyncUnref();
        GetNodeJSInputStream()->AsyncUnref();
        Host->Unref();
    }

    TNodeJSInputStream* GetNodeJSInputStream()
    {
        return static_cast<TNodeJSInputStream*>(InputStack.Bottom());
    }

    TNodeJSOutputStream* GetNodeJSOutputStream()
    {
        return static_cast<TNodeJSOutputStream*>(OutputStack.Bottom());
    }

    void SetCommand(const std::string& commandName, INodePtr arguments)
    {
        DriverRequest.CommandName = commandName;
        DriverRequest.Arguments = arguments->AsMap();
    }

    void SetInputCompression(ECompression compression)
    {
        switch (compression) {
            case ECompression::None:
                break;
            case ECompression::Gzip:
            case ECompression::Deflate:
                InputStack.Add<TZLibDecompress>();
                break;
            case ECompression::LZO:
                InputStack.Add<TLzoDecompress>();
                break;
            case ECompression::LZF:
                InputStack.Add<TLzfDecompress>();
                break;
            case ECompression::Snappy:
                InputStack.Add<TSnappyDecompress>();
                break;
            default:
                YUNREACHABLE();
        }
    }

    void SetInputFormat(INodePtr format)
    {
        DriverRequest.InputFormat = TFormat::FromYson(MoveRV(format));
    }

    void SetOutputCompression(ECompression compression)
    {
        switch (compression) {
            case ECompression::None:
                break;
            case ECompression::Gzip:
                OutputStack.Add<TZLibCompress>(ZLib::GZip, 4, StreamBufferSize);
                break;
            case ECompression::Deflate:
                OutputStack.Add<TZLibCompress>(ZLib::ZLib, 4, StreamBufferSize);
                break;
            case ECompression::LZO:
                OutputStack.Add<TLzoCompress>(StreamBufferSize);
                break;
            case ECompression::LZF:
                OutputStack.Add<TLzfCompress>(StreamBufferSize);
                break;
            case ECompression::Snappy:
                OutputStack.Add<TSnappyCompress>(StreamBufferSize);
                break;
            default:
                YUNREACHABLE();
        }

        OutputStack.Add<TBufferedOutput>(StreamBufferSize);
    }

    void SetOutputFormat(INodePtr format)
    {
        DriverRequest.OutputFormat = TFormat::FromYson(MoveRV(format));
    }

    void Prepare()
    {
        Request.data = this;

        DriverRequest.InputStream = InputStack.Top();
        DriverRequest.OutputStream = OutputStack.Top();
    }

    void Finish()
    {
        FOREACH (auto* current, OutputStack)
        {
            current->Flush();
            current->Finish();
        }
    }
};

Local<Object> ConvertCommandDescriptorToV8Object(const TCommandDescriptor& descriptor)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<Object> result = Object::New();
    result->Set(
        String::New("name"),
        String::New(descriptor.CommandName.c_str()));
    result->Set(
        String::New("input_type"),
        Integer::New(descriptor.InputType.ToValue()));
    result->Set(
        String::New("output_type"),
        Integer::New(descriptor.OutputType.ToValue()));
    result->Set(
        String::New("is_volatile"),
        Boolean::New(descriptor.IsVolatile));
    result->Set(
        String::New("is_heavy"),
        Boolean::New(descriptor.IsHeavy));
    return scope.Close(result);
}

} // namespace

Persistent<FunctionTemplate> TNodeJSDriver::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////

TNodeJSDriver::TNodeJSDriver(Handle<Object> configObject)
    : node::ObjectWrap()
{
    THREAD_AFFINITY_IS_V8();

    bool stillOkay = true;

    INodePtr configNode = ConvertV8ValueToNode(configObject);
    if (!configNode) {
        Message = "Error converting from V8 to YSON";
        return;
    }

    TDriverConfigPtr config;
    try {
        // Qualify namespace to avoid collision with class method New().
        config = ::NYT::New<NDriver::TDriverConfig>();
        config->Load(~configNode);
    } catch (const std::exception& ex) {
        Message = Sprintf("Error loading configuration\n%s", ex.what());
        return;
    }

    try {
        NLog::TLogManager::Get()->Configure(~configNode->AsMap()->GetChild("logging"));
        Driver = CreateDriver(config);
    } catch (const std::exception& ex) {
        Message = Sprintf("Error initializing driver instance\n%s", ex.what());
        return;
    }
}

TNodeJSDriver::~TNodeJSDriver()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSDriver::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TNodeJSDriver::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TNodeJSDriver"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Execute", TNodeJSDriver::Execute);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "FindCommandDescriptor", TNodeJSDriver::FindCommandDescriptor);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "GetCommandDescriptors", TNodeJSDriver::GetCommandDescriptors);

    target->Set(
        String::NewSymbol("TNodeJSDriver"),
        ConstructorTemplate->GetFunction());

#define YTNODE_SET_ENUM(object, type, element) \
    (object)->Set(String::NewSymbol(#type "_" #element), Integer::New(type::element))

    YTNODE_SET_ENUM(target, ECompression, None);
    YTNODE_SET_ENUM(target, ECompression, Gzip);
    YTNODE_SET_ENUM(target, ECompression, Deflate);
    YTNODE_SET_ENUM(target, ECompression, LZO);
    YTNODE_SET_ENUM(target, ECompression, LZF);
    YTNODE_SET_ENUM(target, ECompression, Snappy);

    YTNODE_SET_ENUM(target, EDataType, Null);
    YTNODE_SET_ENUM(target, EDataType, Binary);
    YTNODE_SET_ENUM(target, EDataType, Structured);
    YTNODE_SET_ENUM(target, EDataType, Tabular);

#undef YTNODE_SET_ENUM
}

bool TNodeJSDriver::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

Handle<Value> TNodeJSDriver::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], Object);

    TNodeJSDriver* host = NULL;
    try {
        host = new TNodeJSDriver(Local<Object>::Cast(args[0]));
        host->Wrap(args.This());

        if (host->Driver) {
            return args.This();
        } else {
            return ThrowException(Exception::Error(String::New(~host->Message)));
        }
    } catch (const std::exception& ex) {
        if (host) {
            delete host;
        }

        return ThrowException(Exception::Error(String::New(ex.what())));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSDriver::FindCommandDescriptor(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSDriver* driver =
        ObjectWrap::Unwrap<TNodeJSDriver>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    // Do the work.
    return scope.Close(driver->DoFindCommandDescriptor(args[0].As<String>()));
}

Handle<Value> TNodeJSDriver::DoFindCommandDescriptor(Handle<String> commandNameHandle)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    String::AsciiValue commandNameValue(commandNameHandle);
    Stroka commandName(*commandNameValue, commandNameValue.length());

    auto maybeDescriptor = Driver->FindCommandDescriptor(commandName);
    if (maybeDescriptor) {
        Local<Object> result = ConvertCommandDescriptorToV8Object(*maybeDescriptor);
        return scope.Close(result);
    } else {
        return v8::Null();
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSDriver::GetCommandDescriptors(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSDriver* driver =
        ObjectWrap::Unwrap<TNodeJSDriver>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    return scope.Close(driver->DoGetCommandDescriptors());
}

Handle<Value> TNodeJSDriver::DoGetCommandDescriptors()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<Array> result = Array::New();

    auto descriptors = Driver->GetCommandDescriptors();
    FOREACH (const auto& descriptor, descriptors) {
        Local<Object> resultItem = ConvertCommandDescriptorToV8Object(descriptor);
        result->Set(result->Length(), resultItem);
    }

    return scope.Close(result);
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSDriver::Execute(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YASSERT(args.Length() == 9);

    EXPECT_THAT_IS(args[0], String); // CommandName
    EXPECT_THAT_HAS_INSTANCE(args[1], TNodeJSInputStream); // InputStream
    EXPECT_THAT_IS(args[2], Uint32); // InputCompression
    EXPECT_THAT_IS(args[3], String); // InputFormat
    EXPECT_THAT_HAS_INSTANCE(args[4], TNodeJSOutputStream); // OutputStream
    EXPECT_THAT_IS(args[5], Uint32); // OutputCompression
    EXPECT_THAT_IS(args[6], String); // OutputFormat
    EXPECT_THAT_IS(args[7], Object); // Parameters
    EXPECT_THAT_IS(args[8], Function); // Callback

    // Unwrap arguments.
    TNodeJSDriver* host = ObjectWrap::Unwrap<TNodeJSDriver>(args.This());

    String::AsciiValue commandName(args[0]);
    TNodeJSInputStream* inputStream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args[1].As<Object>());
    ECompression inputCompression =
        (ECompression)args[2]->Uint32Value();
    INodePtr inputFormat =
        ConvertV8StringToNode(args[3].As<String>());
    TNodeJSOutputStream* outputStream =
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args[4].As<Object>());
    ECompression outputCompression =
        (ECompression)args[5]->Uint32Value();
    INodePtr outputFormat =
        ConvertV8StringToNode(args[6].As<String>());
    INodePtr parameters =
        ConvertV8ValueToNode(args[7].As<Object>());
    Local<Function> callback = args[8].As<Function>();

    // Build an atom of work.
    YCHECK(inputFormat);
    YCHECK(outputFormat);
    YCHECK(parameters);
    YCHECK(parameters->GetType() == ENodeType::Map);

    TExecuteRequest* request = new TExecuteRequest(
        host,
        inputStream,
        outputStream,
        callback);

    request->SetCommand(std::string(*commandName, commandName.length()), parameters);

    request->SetInputCompression(inputCompression);
    request->SetInputFormat(inputFormat);

    request->SetOutputCompression(outputCompression);
    request->SetOutputFormat(outputFormat);

    request->Prepare();

    uv_queue_work(
        uv_default_loop(), &request->Request,
        TNodeJSDriver::ExecuteWork, TNodeJSDriver::ExecuteAfter);

    return Undefined();
}

void TNodeJSDriver::ExecuteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();

    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    try {
        request->DriverResponse = request->Host->Driver->Execute(request->DriverRequest);
    } catch (const std::exception& ex) {
        request->Exception = ex.what();
    }
}

void TNodeJSDriver::ExecuteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    request->Finish();

    {
        TryCatch block;

        Local<Value> args[] = {
            Local<Value>::New(v8::Null()),
            Local<Value>::New(v8::Null()),
            Local<Value>::New(v8::Null()),
            Local<Value>::New(v8::Null()),
            Local<Value>::New(v8::Null())
        };

        if (!request->Exception.empty()) {
            args[0] = String::New(~request->Exception);
        } else {
            args[1] = Integer::New(request->DriverResponse.Error.GetCode());
            args[2] = String::New(~request->DriverResponse.Error.GetMessage());
            args[3] = Integer::NewFromUnsigned(request->GetNodeJSInputStream()->BytesCounter);
            args[4] = Integer::NewFromUnsigned(request->GetNodeJSOutputStream()->BytesCounter);
        }

        request->Callback->Call(
            Context::GetCurrent()->Global(),
            ARRAY_SIZE(args),
            args);

        if (block.HasCaught()) {
            node::FatalException(block);
        }
    }

    delete request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
