#include "driver.h"
#include "config.h"
#include "error.h"
#include "future.h"
#include "input_stack.h"
#include "node.h"
#include "output_stack.h"
#include "uv_invoker.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/ytlib/driver/config.h>
#include <yt/ytlib/driver/driver.h>

#include <yt/client/formats/format.h>

#include <yt/core/actions/bind_helpers.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/format.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <util/memory/tempbuf.h>

#include <string>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NDriver;
using namespace NFormats;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

NLogging::TLogger Logger("HttpProxy");

static Persistent<String> DescriptorName;
static Persistent<String> DescriptorInputType;
static Persistent<String> DescriptorInputTypeAsInteger;
static Persistent<String> DescriptorOutputType;
static Persistent<String> DescriptorOutputTypeAsInteger;
static Persistent<String> DescriptorIsVolatile;
static Persistent<String> DescriptorIsHeavy;

class TResponseParametersConsumer
    : public TForwardingYsonConsumer
{
public:
    TResponseParametersConsumer(const Persistent<Function>& callback)
        : Callback_(callback)
    {
        THREAD_AFFINITY_IS_V8();
    }

    ~TResponseParametersConsumer()
    {
        THREAD_AFFINITY_IS_V8();
    }

    virtual void OnMyKeyedItem(TStringBuf keyRef) override
    {
        THREAD_AFFINITY_IS_ANY();

        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        auto builder_ = builder.get();

        builder_->BeginTree();
        Forward(
            builder_,
            BIND(&TResponseParametersConsumer::DoSavePair,
                this,
                Passed(TString(keyRef)),
                Passed(std::move(builder))));
    }

private:
    const Persistent<Function>& Callback_;

    void DoSavePair(TString key, std::unique_ptr<ITreeBuilder> builder)
    {
        auto pair = std::make_pair(std::move(key), builder->EndTree());
        auto future =
            BIND([this, pair = std::move(pair)] () {
                THREAD_AFFINITY_IS_V8();
                HandleScope scope;
                auto keyHandle = String::New(pair.first.c_str());
                auto valueHandle = TNodeWrap::ConstructorTemplate->GetFunction()->NewInstance();
                TNodeWrap::Unwrap(valueHandle)->SetNode(pair.second);
                Invoke(Callback_, keyHandle, valueHandle);
            })
            .AsyncVia(GetUVInvoker())
            .Run();

        // Await for the future, see YT-1095.
        Y_UNUSED(WaitFor(std::move(future)));
    }
};

bool GetDumpErrorIntoResponse(const IMapNodePtr& parameters)
{
    if (auto dumpErrorIntoResponse = parameters->FindChild("dump_error_into_response")) {
        return ConvertTo<bool>(dumpErrorIntoResponse);
    }
    return false;
}

class TExecuteRequest
    : public IAsyncRefCounted
    , public TRefTracked<TExecuteRequest>
{
private:
    const bool Echo_;
    const IDriverPtr Driver_;

    IInvokerPtr IOInvoker_;

    TDriverRequest Request_;

    TNodeJSInputStackPtr InputStack_;
    TNodeJSOutputStackPtr OutputStack_;

    Persistent<Function> ExecuteCallback_;
    Persistent<Function> ParameterCallback_;

    TResponseParametersConsumer ResponseParametersConsumer_;

    NTracing::TTraceContext TraceContext_;

    std::atomic<bool> DestructionInProgress_ = {false};

public:
    TExecuteRequest(
        bool echo,
        IDriverPtr driver,
        TInputStreamWrap* inputStream,
        TOutputStreamWrap* outputStream,
        Handle<Function> executeCallback,
        Handle<Function> parameterCallback)
        : Echo_(echo)
        , Driver_(std::move(driver))
        , IOInvoker_(CreateSerializedInvoker(
            NChunkClient::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker()))
        , InputStack_(New<TNodeJSInputStack>(inputStream, IOInvoker_))
        , OutputStack_(New<TNodeJSOutputStack>(outputStream, IOInvoker_))
        , ExecuteCallback_(Persistent<Function>::New(executeCallback))
        , ParameterCallback_(Persistent<Function>::New(parameterCallback))
        , ResponseParametersConsumer_(ParameterCallback_)
    {
        THREAD_AFFINITY_IS_V8();
    }

    ~TExecuteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        ExecuteCallback_.Dispose();
        ExecuteCallback_.Clear();

        ParameterCallback_.Dispose();
        ParameterCallback_.Clear();
    }

    void Ref()
    {
        AsyncRef();
    }

    void Unref()
    {
        AsyncUnref();
    }

    void Prepare(
        ui64 requestId,
        TString commandName,
        TString authenticatedUser,
        INodePtr parameters,
        ECompression inputCompression,
        ECompression outputCompression)
    {
        THREAD_AFFINITY_IS_V8();

        Request_.Id = requestId;
        Request_.CommandName = std::move(commandName);
        Request_.AuthenticatedUser = std::move(authenticatedUser);
        Request_.Parameters = parameters->AsMap();

        auto trace = Request_.Parameters->FindChild("trace");
        if (trace && ConvertTo<bool>(trace)) {
            TraceContext_ = NTracing::CreateRootTraceContext();
            if (requestId) {
                TraceContext_ = NTracing::TTraceContext(
                    requestId,
                    TraceContext_.GetSpanId(),
                    TraceContext_.GetParentSpanId());
            }
        }

        InputStack_->AddCompression(inputCompression);
        Request_.InputStream = InputStack_;

        OutputStack_->AddCompression(outputCompression);
        Request_.OutputStream = OutputStack_;

        Request_.ResponseParametersConsumer = &ResponseParametersConsumer_;
    }

    Handle<Value> Run()
    {
        THREAD_AFFINITY_IS_V8();
        // Assume outer handle scope.

        // May acquire sync-reference.
        auto this_ = MakeStrong(this);

        TFuture<void> future;
        auto wrappedFuture = TFutureWrap::ConstructorTemplate->GetFunction()->NewInstance();

        if (Y_LIKELY(!Echo_)) {
            NTracing::TTraceContextGuard guard(TraceContext_);
            future = Driver_->Execute(Request_);
        } else {
            future =
                BIND(&TExecuteRequest::PipeInputToOutput, this_)
                .AsyncVia(IOInvoker_)
                .Run();
        }

        future
            .Apply(BIND(&TExecuteRequest::OnResponse1, this_))
            .Subscribe(
                BIND(&TExecuteRequest::OnResponse2, this_)
                .Via(GetUVInvoker()));

        TFutureWrap::Unwrap(wrappedFuture)->SetFuture(std::move(future));

        return wrappedFuture;
    }

private:
    void PipeInputToOutput()
    {
        THREAD_AFFINITY_IS_ANY();

        TTempBuf buffer;
        auto inputStream = CreateSyncAdapter(Request_.InputStream);
        auto outputStream = CreateBufferedSyncAdapter(Request_.OutputStream);

        while (size_t length = inputStream->Load(buffer.Data(), buffer.Size())) {
            outputStream->Write(buffer.Data(), length);
        }

        outputStream->Finish();
    }

    TFuture<void> OnResponse1(const TErrorOr<void>& response)
    {
        if (!response.IsOK() && GetDumpErrorIntoResponse(Request_.Parameters)) {
            TString errorMessage;
            TStringOutput errorStream(errorMessage);

            auto formatAttributes = CreateEphemeralAttributes();
            formatAttributes->SetYson("format", TYsonString("pretty"));

            auto consumer = CreateConsumerForFormat(
                TFormat(EFormatType::Json, formatAttributes.get()),
                EDataType::Structured,
                &errorStream);

            Serialize(response, consumer.get());
            consumer->Flush();
            errorStream.Finish();

            TString delimiter;
            delimiter.append('\n');
            delimiter.append(80, '=');
            delimiter.append('\n');

            OutputStack_->Write(TSharedRef::FromString("\n"));
            OutputStack_->Write(TSharedRef::FromString(delimiter));
            OutputStack_->Write(TSharedRef::FromString(errorMessage));
            OutputStack_->Write(TSharedRef::FromString(delimiter));
        }

        return OutputStack_->Close().Apply(BIND([=] (const TErrorOr<void>&) {
            return MakeFuture(response);
        }));
    }

    void OnResponse2(const TErrorOr<void>& response)
    {
        THREAD_AFFINITY_IS_V8();
        HandleScope scope;

        // XXX(sandello): We cannot represent ui64 precisely in V8, because there
        // is no native ui64 integer type. So we convert ui64 to double (v8::Number)
        // to precisely represent all integers up to 2^52
        // (see http://en.wikipedia.org/wiki/Double_precision).
        double bytesIn = InputStack_->GetBytes();
        double bytesOut = OutputStack_->GetBytes();

        Invoke(ExecuteCallback_,
            ConvertErrorToV8(response),
            Number::New(bytesIn),
            Number::New(bytesOut));
    }

    void SyncRef()
    {
        YCHECK(!DestructionInProgress_);
    }

    void SyncUnref()
    {
        bool expected = false;
        YCHECK(DestructionInProgress_.compare_exchange_strong(expected, true));

        BIND([this] () { delete this; })
            .Via(GetUVInvoker())
            .Run();
    }
};

// Assuming presence of outer HandleScope.
Local<Object> ConvertCommandDescriptorToV8Object(const TCommandDescriptor& descriptor)
{
    Local<Object> result = Object::New();
    result->Set(
        DescriptorName,
        String::New(descriptor.CommandName.c_str()),
        v8::ReadOnly);
    result->Set(
        DescriptorInputType,
        String::New(to_lower(ToString(descriptor.InputType)).c_str()),
        v8::ReadOnly);
    result->Set(
        DescriptorInputTypeAsInteger,
        Integer::New(static_cast<int>(descriptor.InputType)),
        static_cast<v8::PropertyAttribute>(v8::ReadOnly | v8::DontEnum));
    result->Set(
        DescriptorOutputType,
        String::New(to_lower(ToString(descriptor.OutputType)).c_str()),
        v8::ReadOnly);
    result->Set(
        DescriptorOutputTypeAsInteger,
        Integer::New(static_cast<int>(descriptor.OutputType)),
        static_cast<v8::PropertyAttribute>(v8::ReadOnly | v8::DontEnum));
    result->Set(
        DescriptorIsVolatile,
        Boolean::New(descriptor.Volatile),
        v8::ReadOnly);
    result->Set(
        DescriptorIsHeavy,
        Boolean::New(descriptor.Heavy),
        v8::ReadOnly);
    return result;
}

// Assuming presence of outer HandleScope.
template <class E>
void ExportEnumeration(
    const Handle<Object>& target,
    const char* name)
{
    auto values = TEnumTraits<E>::GetDomainValues();
    Local<Array> mapping = Array::New();

    for (auto value : values) {
        auto key = TString::Join(name, "_", TEnumTraits<E>::FindLiteralByValue(value)->data());
        auto keyHandle = String::NewSymbol(key.c_str());
        auto valueHandle = Integer::New(static_cast<int>(value));
        target->Set(
            keyHandle,
            valueHandle,
            static_cast<v8::PropertyAttribute>(v8::ReadOnly | v8::DontDelete));
        mapping->Set(valueHandle, keyHandle);
    }
    target->Set(String::NewSymbol(name), mapping);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TDriverWrap::ConstructorTemplate;

TDriverWrap::TDriverWrap(bool echo, IDriverPtr driver)
    : node::ObjectWrap()
    , Echo(echo)
    , Driver(std::move(driver))
{
    THREAD_AFFINITY_IS_V8();
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

    ExportEnumeration<ECompression>(target, "ECompression");
    ExportEnumeration<EDataType>(target, "EDataType");
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

    YCHECK(args.Length() == 2);

    EXPECT_THAT_IS(args[0], Boolean);
    EXPECT_THAT_IS(args[1], Object);

    try {
        bool echo = args[0]->BooleanValue();

        auto configNode = ConvertV8ValueToNode(args[1].As<Object>());
        if (!configNode) {
            return ThrowException(Exception::Error(String::New(
                "Error converting from V8 to YSON")));
        }

        auto config = NYT::New<NYT::NNodeJS::THttpProxyConfig>();
        config->Load(configNode);

        auto driver = CreateDriver(config->Driver);

        auto wrap = new TDriverWrap(echo, std::move(driver));
        wrap->Wrap(args.This());

        return scope.Close(args.This());
    } catch (const std::exception& ex) {
        return ThrowException(Exception::Error(String::New(ex.what())));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TDriverWrap::FindCommandDescriptor(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap object.
    auto* driver = ObjectWrap::Unwrap<TDriverWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    // Unwrap arguments.
    String::Utf8Value commandNameValue(args[0]);
    TString commandName(*commandNameValue, commandNameValue.length());

    // Do the work.
    return scope.Close(driver->DoFindCommandDescriptor(commandName));
}

Handle<Value> TDriverWrap::DoFindCommandDescriptor(const TString& commandName)
{
    auto maybeDescriptor = Driver->FindCommandDescriptor(commandName);
    if (maybeDescriptor) {
        return ConvertCommandDescriptorToV8Object(*maybeDescriptor);
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
    auto* driver = ObjectWrap::Unwrap<TDriverWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(driver->DoGetCommandDescriptors());
}

Handle<Value> TDriverWrap::DoGetCommandDescriptors()
{
    Local<Array> result = Array::New();

    auto descriptors = Driver->GetCommandDescriptors();
    for (const auto& descriptor : descriptors) {
        Local<Object> resultItem = ConvertCommandDescriptorToV8Object(descriptor);
        result->Set(result->Length(), resultItem);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TDriverWrap::Execute(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YCHECK(args.Length() == 10);

    EXPECT_THAT_IS(args[0], String); // CommandName
    EXPECT_THAT_IS(args[1], String); // AuthenticatedUser

    EXPECT_THAT_HAS_INSTANCE(args[2], TInputStreamWrap); // InputStream
    EXPECT_THAT_IS(args[3], Uint32); // InputCompression

    EXPECT_THAT_HAS_INSTANCE(args[4], TOutputStreamWrap); // OutputStream
    EXPECT_THAT_IS(args[5], Uint32); // OutputCompression

    EXPECT_THAT_HAS_INSTANCE(args[6], TNodeWrap); // Parameters

    EXPECT_THAT_IS(args[8], Function); // ExecuteCallback
    EXPECT_THAT_IS(args[9], Function); // ParameterCallback

    // Unwrap arguments.
    auto* wrap = ObjectWrap::Unwrap<TDriverWrap>(args.This());

    String::AsciiValue commandName(args[0]);
    String::AsciiValue authenticatedUser(args[1]);

    auto* inputStream = ObjectWrap::Unwrap<TInputStreamWrap>(args[2].As<Object>());
    auto inputCompression = (ECompression)args[3]->Uint32Value();

    auto* outputStream = ObjectWrap::Unwrap<TOutputStreamWrap>(args[4].As<Object>());
    auto outputCompression = (ECompression)args[5]->Uint32Value();

    auto parameters = TNodeWrap::UnwrapNode(args[6]);

    ui64 requestId = 0;

    if (node::Buffer::HasInstance(args[7])) {
        const char* buffer = node::Buffer::Data(args[7].As<Object>());
        size_t length = node::Buffer::Length(args[7].As<Object>());
        if (length == 8) {
            requestId = __builtin_bswap64(*(ui64*)buffer);
        }
    }

    auto executeCallback = args[8].As<Function>();
    auto parameterCallback = args[9].As<Function>();

    // Build an atom of work.
    YCHECK(parameters);
    YCHECK(parameters->GetType() == ENodeType::Map);

    TIntrusivePtr<TExecuteRequest> request(new TExecuteRequest(
        wrap->Echo,
        wrap->Driver,
        inputStream,
        outputStream,
        executeCallback,
        parameterCallback));

    request->Prepare(
        requestId,
        TString(*commandName, commandName.length()),
        TString(*authenticatedUser, authenticatedUser.length()),
        std::move(parameters),
        inputCompression,
        outputCompression);

    return scope.Close(request->Run());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

