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

DECLARE_ENUM(ECompression,
    (None)
    (Gzip)
    (Deflate)
    (LZO)
    (LZF)
    (Snappy)
);

// TODO(sandello): Refactor this huge mess.
struct TExecuteRequest
{
    uv_work_t Request;
    TNodeJSDriver* Host;

    TNodeJSInputStream* InputStream;
    ECompression InputCompression;

    TNodeJSOutputStream* OutputStream;
    ECompression OutputCompression;

    Persistent<Function> Callback;

    Stroka Exception;

    TDriverRequest DriverRequest;
    TDriverResponse DriverResponse;

    TExecuteRequest(
        TNodeJSDriver* host,
        TNodeJSInputStream* inputStream,
        ECompression inputStreamCompression,
        TNodeJSOutputStream* outputStream,
        ECompression outputStreamCompression,
        Handle<Function> callback)
        : Host(host)
        , InputStream(inputStream)
        , InputCompression(inputStreamCompression)
        , OutputStream(outputStream)
        , OutputCompression(outputStreamCompression)
        , Callback(Persistent<Function>::New(callback))
    {
        THREAD_AFFINITY_IS_V8();

        YASSERT(Host);
        YASSERT(InputStream);
        YASSERT(OutputStream);

        SwitchStreams(InputStream, OutputStream);

        Host->Ref();
        InputStream->AsyncRef(true);
        OutputStream->AsyncRef(true);
    }

    ~TExecuteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        Callback.Dispose();
        Callback.Clear();

        OutputStream->AsyncUnref();
        InputStream->AsyncUnref();
        Host->Unref();
    }

    void SwitchStreams(TInputStream* input, TOutputStream* output)
    {
        DriverRequest.InputStream = input;
        DriverRequest.OutputStream = output;
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

template <class T, size_t N>
class TCustomStreamStack
{
private:
    static const size_t OwnershipMask = (size_t)(0x1);
    static const size_t PointerMask = ~(size_t)(0x1);

public:
    TCustomStreamStack()
        : Head(Stack + N)
    { }

    ~TCustomStreamStack()
    {
        T** end = Stack + N;
        for (T** current = Head; current != end; ++current) {
            if ((size_t)(*current) & OwnershipMask) {
                delete (T*)((size_t)(*current) & PointerMask);
            }
        }
    }

    template <class S>
    void Add(S* stream, bool takeOwnership = true)
    {
        YASSERT(((size_t)stream & OwnershipMask) == 0);
        YASSERT((Head > Stack));

        *--Head = stream;
        if (takeOwnership) {
            *Head = (T*)((size_t)(*Head) | OwnershipMask);
        }
    }

    T* Top() const
    {
        T* const* end = Stack + N;
        return Head == end ? NULL : (T*)((size_t)(*Head) & PointerMask);
    }

    T* Bottom() const
    {
        T* const* end = Stack + N;
        return Head == end ? NULL : (T*)((size_t)(*(end - 1)) & PointerMask);
    }

private:
    typedef T* TPtr;
    TPtr Stack[N];
    TPtr* Head;
};

static const size_t StreamBufferSize = 32 * 1024;

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
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "GetCommandDescriptor", TNodeJSDriver::GetCommandDescriptors);

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

    Local<Array> result;

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
    ECompression inputStreamCompression =
        (ECompression)args[2]->Uint32Value();
    INodePtr inputFormat =
        ConvertV8StringToNode(args[3].As<String>());
    TNodeJSOutputStream* outputStream =
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args[4].As<Object>());
    ECompression outputStreamCompression =
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
        inputStreamCompression,
        outputStream,
        outputStreamCompression,
        callback);

    // Fill in TDriverRequest structure.
    request->DriverRequest.CommandName = std::string(*commandName, commandName.length());
    request->DriverRequest.InputFormat = TFormat::FromYson(inputFormat);
    request->DriverRequest.OutputFormat = TFormat::FromYson(outputFormat);
    // TODO(sandello): Arguments -> Parameters
    request->DriverRequest.Arguments = parameters->AsMap();

    request->Request.data = request;
    uv_queue_work(
        uv_default_loop(), &request->Request,
        TNodeJSDriver::ExecuteWork, TNodeJSDriver::ExecuteAfter);

    return Undefined();
}

void TNodeJSDriver::ExecuteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();
    
    try {
        ExecuteWorkUnsafe(workRequest);
    } catch(const std::exception& ex) {
        // TODO(sandello): Better logging here.
        Cerr << "*** Unhandled exception in TNodeJSDriver::ExecuteWork: " << ex.what();
    }    
}

void TNodeJSDriver::ExecuteWorkUnsafe(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();

    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    TCustomStreamStack<TInputStream, 2> inputStream;
    TCustomStreamStack<TOutputStream, 3> outputStream;

    inputStream.Add(request->InputStream, false);
    outputStream.Add(request->OutputStream, false);
    outputStream.Add(new TBufferedOutput(outputStream.Top(), StreamBufferSize));

    switch (request->InputCompression) {
        case ECompression::None:
            break;
        case ECompression::Gzip:
        case ECompression::Deflate:
            inputStream.Add(new TZLibDecompress(inputStream.Top()));
            break;
        case ECompression::LZO:
            inputStream.Add(new TLzoDecompress(inputStream.Top()));
            break;
        case ECompression::LZF:
            inputStream.Add(new TLzfDecompress(inputStream.Top()));
            break;
        case ECompression::Snappy:
            inputStream.Add(new TSnappyDecompress(inputStream.Top()));
            break;
        default:
            YUNREACHABLE();
    }

    switch (request->OutputCompression) {
        case ECompression::None:
            break;
        case ECompression::Gzip:
            outputStream.Add(new TZLibCompress(outputStream.Top(), ZLib::GZip, 4, StreamBufferSize));
            break;
        case ECompression::Deflate:
            outputStream.Add(new TZLibCompress(outputStream.Top(), ZLib::ZLib, 4, StreamBufferSize));
            break;
        case ECompression::LZO:
            outputStream.Add(new TLzoCompress(outputStream.Top(), StreamBufferSize));
            break;
        case ECompression::LZF:
            outputStream.Add(new TLzfCompress(outputStream.Top(), StreamBufferSize));
            break;
        case ECompression::Snappy:
            outputStream.Add(new TSnappyCompress(outputStream.Top(), StreamBufferSize));
            break;
        default:
            YUNREACHABLE();
    }

    request->SwitchStreams(inputStream.Top(), outputStream.Top());
    try {
        request->DriverResponse = request->Host->Driver->Execute(request->DriverRequest);
    } catch (const std::exception& ex) {
        request->Exception = ex.what();
    }
    request->SwitchStreams(inputStream.Bottom(), outputStream.Bottom());
}

void TNodeJSDriver::ExecuteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    {
        TryCatch block;

        Local<Value> args[] = {
            Local<Value>::New(v8::Null()),
            Local<Value>::New(v8::Null()),
            Local<Value>::New(v8::Null())
        };

        if (!request->Exception.empty()) {
            args[0] = String::New(~request->Exception);
        } else {
            args[1] = Integer::New(request->DriverResponse.Error.GetCode());
            args[2] = String::New(~request->DriverResponse.Error.GetMessage());
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
