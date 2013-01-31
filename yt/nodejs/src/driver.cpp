#include "config.h"
#include "driver.h"
#include "node.h"
#include "error.h"
#include "input_stream.h"
#include "input_stack.h"
#include "output_stream.h"
#include "output_stack.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/convert.h>

#include <ytlib/logging/log.h>

#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>

#include <ytlib/formats/format.h>

#include <util/memory/tempbuf.h>

#include <string>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NDriver;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

namespace {

NLog::TLogger Logger("HttpProxy");

static Persistent<String> DescriptorName;
static Persistent<String> DescriptorInputType;
static Persistent<String> DescriptorInputTypeAsInteger;
static Persistent<String> DescriptorOutputType;
static Persistent<String> DescriptorOutputTypeAsInteger;
static Persistent<String> DescriptorIsVolatile;
static Persistent<String> DescriptorIsHeavy;

// TODO(sandello): Refactor this huge mess.
struct TExecuteRequest
{
    uv_work_t Request;
    TDriverWrap* Host;

    TNodeJSInputStack InputStack;
    TNodeJSOutputStack OutputStack;

    Persistent<Function> Callback;

    TDriverRequest DriverRequest;
    TDriverResponse DriverResponse;

    TExecuteRequest(
        TDriverWrap* host,
        TInputStreamWrap* inputStream,
        TOutputStreamWrap* outputStream,
        Handle<Function> callback)
        : Host(host)
        , InputStack(inputStream)
        , OutputStack(outputStream)
        , Callback(Persistent<Function>::New(callback))
    {
        THREAD_AFFINITY_IS_V8();
        YASSERT(Host);
        Host->Ref();
    }

    ~TExecuteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        Callback.Dispose();
        Callback.Clear();

        Host->Unref();
    }

    TInputStreamWrap* GetNodeJSInputStream()
    {
        return InputStack.GetBaseStream();
    }

    TOutputStreamWrap* GetNodeJSOutputStream()
    {
        return OutputStack.GetBaseStream();
    }

    void SetCommand(const std::string& commandName, INodePtr arguments)
    {
        DriverRequest.CommandName = commandName;
        DriverRequest.Arguments = arguments->AsMap();
    }

    void SetInputCompression(ECompression compression)
    {
        InputStack.AddCompression(compression);
    }

    void SetInputFormat(INodePtr format)
    {
        DriverRequest.InputFormat = ConvertTo<TFormat>(std::move(format));
    }

    void SetOutputCompression(ECompression compression)
    {
        OutputStack.AddCompression(compression);
    }

    void SetOutputFormat(INodePtr format)
    {
        DriverRequest.OutputFormat = ConvertTo<TFormat>(std::move(format));
    }

    void Prepare()
    {
        Request.data = this;

        DriverRequest.InputStream = &InputStack;
        DriverRequest.OutputStream = &OutputStack;
    }

    void Flush()
    {
        FOREACH (auto* current, OutputStack)
        {
            current->Flush();
        }
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
        DescriptorName,
        String::New(descriptor.CommandName.c_str()),
        v8::ReadOnly);
    result->Set(
        DescriptorInputType,
        String::New(to_lower(descriptor.InputType.ToString()).c_str()),
        v8::ReadOnly);
    result->Set(
        DescriptorInputTypeAsInteger,
        Integer::New(descriptor.InputType.ToValue()),
        static_cast<v8::PropertyAttribute>(v8::ReadOnly | v8::DontEnum));
    result->Set(
        DescriptorOutputType,
        String::New(to_lower(descriptor.OutputType.ToString()).c_str()),
        v8::ReadOnly);
    result->Set(
        DescriptorOutputTypeAsInteger,
        Integer::New(descriptor.OutputType.ToValue()),
        static_cast<v8::PropertyAttribute>(v8::ReadOnly | v8::DontEnum));
    result->Set(
        DescriptorIsVolatile,
        Boolean::New(descriptor.IsVolatile),
        v8::ReadOnly);
    result->Set(
        DescriptorIsHeavy,
        Boolean::New(descriptor.IsHeavy),
        v8::ReadOnly);
    return scope.Close(result);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TDriverWrap::ConstructorTemplate;

TDriverWrap::TDriverWrap(bool echo, Handle<Object> configObject)
    : node::ObjectWrap()
    , Echo(echo)
{
    THREAD_AFFINITY_IS_V8();

    INodePtr configNode = ConvertV8ValueToNode(configObject);
    if (!configNode) {
        Message = "Error converting from V8 to YSON";
        return;
    }

    NNodeJS::THttpProxyConfigPtr config;
    try {
        // Qualify namespace to avoid collision with class method New().
        config = NYT::New<NYT::NNodeJS::THttpProxyConfig>();
        config->Load(configNode);
    } catch (const std::exception& ex) {
        Message = Sprintf("Error loading configuration\n%s", ex.what());
        return;
    }

    try {
        Driver = CreateDriver(config->Driver);
    } catch (const std::exception& ex) {
        Message = Sprintf("Error initializing driver instance\n%s", ex.what());
        return;
    }
}

TDriverWrap::~TDriverWrap()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TDriverWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    DescriptorName = NODE_PSYMBOL("name");
    DescriptorInputType = NODE_PSYMBOL("input_type");
    DescriptorInputTypeAsInteger = NODE_PSYMBOL("input_type_as_integer");
    DescriptorOutputType = NODE_PSYMBOL("output_type");
    DescriptorOutputTypeAsInteger = NODE_PSYMBOL("output_type_as_integer");
    DescriptorIsVolatile = NODE_PSYMBOL("is_volatile");
    DescriptorIsHeavy = NODE_PSYMBOL("is_heavy");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TDriverWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TDriverWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Execute", TDriverWrap::Execute);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "FindCommandDescriptor", TDriverWrap::FindCommandDescriptor);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "GetCommandDescriptors", TDriverWrap::GetCommandDescriptors);

    target->Set(
        String::NewSymbol("TDriverWrap"),
        ConstructorTemplate->GetFunction());

    auto compressionValues = ECompression::GetDomainValues();
    Local<Array> compressionMapping = Array::New();
    FOREACH (auto& value, compressionValues) {
        Stroka key = Stroka::Join("ECompression_", ECompression::GetLiteralByValue(value));
        auto keyHandle = String::NewSymbol(key.c_str());
        auto valueHandle = Integer::New(value);
        target->Set(keyHandle, valueHandle);
        compressionMapping->Set(valueHandle, keyHandle);
    }
    target->Set(String::NewSymbol("ECompression"), compressionMapping);

    auto dataTypeValues = EDataType::GetDomainValues();
    Local<Array> dataTypeMapping = Array::New();
    FOREACH (auto& value, dataTypeValues) {
        Stroka key = Stroka::Join("EDataType_", EDataType::GetLiteralByValue(value));
        auto keyHandle = String::NewSymbol(key.c_str());
        auto valueHandle = Integer::New(value);
        target->Set(keyHandle, valueHandle);
        dataTypeMapping->Set(valueHandle, keyHandle);
    }
    target->Set(String::NewSymbol("EDataType"), dataTypeMapping);
}

bool TDriverWrap::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

Handle<Value> TDriverWrap::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 2);

    EXPECT_THAT_IS(args[0], Boolean);
    EXPECT_THAT_IS(args[1], Object);

    TDriverWrap* host = NULL;
    try {
        host = new TDriverWrap(
            args[0]->BooleanValue(),
            args[1].As<Object>());
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

Handle<Value> TDriverWrap::FindCommandDescriptor(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TDriverWrap* driver =
        ObjectWrap::Unwrap<TDriverWrap>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    // Do the work.
    return scope.Close(driver->DoFindCommandDescriptor(args[0].As<String>()));
}

Handle<Value> TDriverWrap::DoFindCommandDescriptor(Handle<String> commandNameHandle)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    String::AsciiValue commandNameValue(commandNameHandle);
    Stroka commandName(*commandNameValue, commandNameValue.length());

    auto maybeDescriptor = Driver->FindCommandDescriptor(commandName);
    if (maybeDescriptor) {
        return scope.Close(
            ConvertCommandDescriptorToV8Object(*maybeDescriptor));
    } else {
        return v8::Null();
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TDriverWrap::GetCommandDescriptors(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TDriverWrap* driver =
        ObjectWrap::Unwrap<TDriverWrap>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    return scope.Close(driver->DoGetCommandDescriptors());
}

Handle<Value> TDriverWrap::DoGetCommandDescriptors()
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

Handle<Value> TDriverWrap::Execute(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YASSERT(args.Length() == 9);

    EXPECT_THAT_IS(args[0], String); // CommandName

    EXPECT_THAT_HAS_INSTANCE(args[1], TInputStreamWrap); // InputStream
    EXPECT_THAT_IS(args[2], Uint32); // InputCompression
    EXPECT_THAT_HAS_INSTANCE(args[3], TNodeWrap); // InputFormat

    EXPECT_THAT_HAS_INSTANCE(args[4], TOutputStreamWrap); // OutputStream
    EXPECT_THAT_IS(args[5], Uint32); // OutputCompression
    EXPECT_THAT_HAS_INSTANCE(args[6], TNodeWrap); // OutputFormat)

    EXPECT_THAT_HAS_INSTANCE(args[7], TNodeWrap); // Parameters
    EXPECT_THAT_IS(args[8], Function); // Callback

    // Unwrap arguments.
    TDriverWrap* host = ObjectWrap::Unwrap<TDriverWrap>(args.This());

    String::AsciiValue commandName(args[0]);

    TInputStreamWrap* inputStream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args[1].As<Object>());
    ECompression inputCompression =
        (ECompression)args[2]->Uint32Value();
    INodePtr inputFormat = TNodeWrap::UnwrapNode(args[3]);

    TOutputStreamWrap* outputStream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args[4].As<Object>());
    ECompression outputCompression =
        (ECompression)args[5]->Uint32Value();
    INodePtr outputFormat = TNodeWrap::UnwrapNode(args[6]);

    INodePtr parameters = TNodeWrap::UnwrapNode(args[7]);
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
        TDriverWrap::ExecuteWork, TDriverWrap::ExecuteAfter);

    return Undefined();
}

void TDriverWrap::ExecuteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();

    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    if (LIKELY(!request->Host->Echo)) {
        request->DriverResponse = request->Host->Driver->Execute(request->DriverRequest);
    } else {
        TTempBuf buffer;
        auto inputStream = request->DriverRequest.InputStream;
        auto outputStream = request->DriverRequest.OutputStream;

        while (size_t bytesRead = inputStream->Read(buffer.Data(), buffer.Size())) {
            outputStream->Write(buffer.Data(), bytesRead);
        }

        request->DriverResponse = TDriverResponse();
    }
}

void TDriverWrap::ExecuteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    if (!request->OutputStack.HasAnyData()) {
        // In this case we have to prematurely destroy the stream to avoid
        // writing middleware-induced framing overhead.
        request->OutputStack.GetBaseStream()->DoDestroy();
    } else {
        try {
            request->Finish();
        } catch (const std::exception& ex) {
            LOG_DEBUG(TError(ex), "Ignoring exception while closing driver output stream");
        }
    }

    char buffer[32]; // This should be enough to stringify ui64.
    size_t length;

    TryCatch catcher;

    Handle<Value> args[] = {
        Local<Value>::New(v8::Null()),
        Local<Value>::New(v8::Null()),
        Local<Value>::New(v8::Null())
    };

    args[0] = ConvertErrorToV8(request->DriverResponse.Error);

    // XXX(sandello): Since counters are ui64 we cannot represent
    // them precisely in V8. Therefore we pass them as strings
    // because they are used only for debugging purposes.
    auto* nodejsInputStream = request->InputStack.GetBaseStream();
    length = ToString(
        nodejsInputStream->GetBytesEnqueued(),
        buffer,
        sizeof(buffer));
    args[1] = String::New(buffer, length);

    auto* nodejsOutputStream = request->OutputStack.GetBaseStream();
    length = ToString(
        nodejsOutputStream->GetBytesEnqueued(),
        buffer,
        sizeof(buffer));
    args[2] = String::New(buffer, length);

    request->Callback->Call(
        Context::GetCurrent()->Global(),
        ARRAY_SIZE(args),
        args);

    delete request;

    if (catcher.HasCaught()) {
        node::FatalException(catcher);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
