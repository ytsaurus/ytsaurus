#include "driver.h"

#include <ytlib/logging/log_manager.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/formats/format.h>

#include <string>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NDriver;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

namespace {
struct TExecuteRequest
{
    uv_work_t Request;
    TNodeJSDriver* Host;
    TNodeJSInputStream* InputStream;
    TNodeJSOutputStream* OutputStream;

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
        , InputStream(inputStream)
        , OutputStream(outputStream)
        , Callback(Persistent<Function>::New(callback))
    {
        THREAD_AFFINITY_IS_V8();

        YASSERT(Host);
        YASSERT(InputStream);
        YASSERT(OutputStream);

        DriverRequest.InputStream = InputStream;
        DriverRequest.OutputStream = OutputStream;

        Host->Ref();
        InputStream->Ref();
        OutputStream->Ref();
    }

    ~TExecuteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        Callback.Dispose();
        Callback.Clear();

        OutputStream->Unref();
        InputStream->Unref();
        Host->Unref();
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
    return scope.Close(result);
}
} // namespace

Persistent<FunctionTemplate> TNodeJSDriver::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////

TNodeJSDriver::TNodeJSDriver(const NYTree::TYson& configuration)
    : node::ObjectWrap()
{
    THREAD_AFFINITY_IS_V8();

    bool stillOkay = true;

    INodePtr configNode;
    if (stillOkay) {
        try {
            configNode = DeserializeFromYson(configuration);
        } catch (const std::exception& ex) {
            Message = Sprintf("Error reading configuration\n%s", ex.what());
            stillOkay = false;
        }
    }

    TDriverConfigPtr config;
    if (stillOkay) {
        try {
            // Qualify namespace to avoid collision with v8-New().
            config = ::NYT::New<NDriver::TDriverConfig>();
            config->Load(~configNode);
        } catch (const std::exception& ex) {
            Message = Sprintf("Error parsing configuration\n%s", ex.what());
            stillOkay = false;
        }
    }

    if (stillOkay) {
        try {
            NLog::TLogManager::Get()->Configure(~configNode->AsMap()->GetChild("logging"));
            Driver = CreateDriver(config);
        } catch (const std::exception& ex) {
            Message = Sprintf("Error initializing driver instance\n%s", ex.what());
            stillOkay = false;
        }
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

    Local<Object> dataType = Object::New();
#define YTNODE_SET_ENUM(object, type, element) \
    (object)->Set(type::element, String::New(type::GetLiteralByValue(type::element)))
    YTNODE_SET_ENUM(dataType, EDataType, Null);
    YTNODE_SET_ENUM(dataType, EDataType, Binary);
    YTNODE_SET_ENUM(dataType, EDataType, Structured);
    YTNODE_SET_ENUM(dataType, EDataType, Tabular);
#undef YTNODE_SET_ENUM

    target->Set(
        String::New("EDataType"),
        dataType);
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
    EXPECT_THAT_IS(args[0], String);

    String::AsciiValue configuration(args[0]);

    TNodeJSDriver* host = new TNodeJSDriver(
        Stroka(*configuration, configuration.length()));
    host->Wrap(args.This());

    if (host->Driver) {
        return args.This();
    } else {
        return ThrowException(Exception::Error(String::New(~host->Message)));
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
    YASSERT(args.Length() == 7);

    EXPECT_THAT_IS(args[0], String); // CommandName
    EXPECT_THAT_HAS_INSTANCE(args[1], TNodeJSInputStream); // InputStream
    EXPECT_THAT_IS(args[2], String); // InputFormat
    EXPECT_THAT_HAS_INSTANCE(args[3], TNodeJSOutputStream); // OutputStream
    EXPECT_THAT_IS(args[4], String); // OutputFormat
    EXPECT_THAT_IS(args[5], Object); // Parameters
    EXPECT_THAT_IS(args[6], Function); // Callback

    // Unwrap arguments.
    String::AsciiValue commandName(args[0]);
    TNodeJSInputStream* inputStream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args[1].As<Object>());
    INodePtr inputFormat =
        ConvertV8StringToYson(args[2].As<String>());
    TNodeJSOutputStream* outputStream =
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args[3].As<Object>());
    INodePtr outputFormat =
        ConvertV8StringToYson(args[4].As<String>());
    INodePtr parameters =
        ConvertV8ValueToYson(args[5].As<Object>());
    Local<Function> callback = args[6].As<Function>();

    // Build an atom of work.
    YCHECK(inputFormat);
    YCHECK(outputFormat);
    YCHECK(parameters);
    YCHECK(parameters->GetType() == ENodeType::Map);

    TExecuteRequest* request = new TExecuteRequest(
        ObjectWrap::Unwrap<TNodeJSDriver>(args.This()),
        inputStream,
        outputStream,
        callback);

    // Fill in TDriverRequest structure.
    request->DriverRequest.CommandName = std::string(*commandName, commandName.length());
    request->DriverRequest.InputFormat = TFormat::FromYson(inputFormat);
    request->DriverRequest.OutputFormat = TFormat::FromYson(outputFormat);
    // TODO(sandello): Arguments -> Parameters
    request->DriverRequest.Arguments = parameters->AsMap();

    uv_queue_work(
        uv_default_loop(), &request->Request,
        TNodeJSDriver::ExecuteWork, TNodeJSDriver::ExecuteAfter);

    return Undefined();
}

void TNodeJSDriver::ExecuteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();
    TExecuteRequest* request =
        container_of(workRequest, TExecuteRequest, Request);

    TInputStream* inputStream = request->DriverRequest.InputStream;
    TOutputStream* outputStream = request->DriverRequest.OutputStream;

    TBufferedOutput bufferedOutputStream(outputStream);

    request->DriverRequest.OutputStream = &bufferedOutputStream;
    try {
        request->DriverResponse = request->Host->Driver->Execute(request->DriverRequest);
    } catch (const std::exception& ex) {
        request->Exception = ex.what();
    }
    request->DriverRequest.OutputStream = outputStream;
}

void TNodeJSDriver::ExecuteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TExecuteRequest* request =
        container_of(workRequest, TExecuteRequest, Request);

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
