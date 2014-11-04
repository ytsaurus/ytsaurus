#include "config.h"
#include "driver.h"
#include "node.h"
#include "error.h"
#include "input_stream.h"
#include "input_stack.h"
#include "output_stream.h"
#include "output_stack.h"

#include <core/misc/error.h>
#include <core/misc/lazy_ptr.h>
#include <core/misc/format.h>

#include <core/concurrency/async_stream.h>

#include <core/ytree/node.h>
#include <core/ytree/convert.h>

#include <core/logging/log.h>

#include <ytlib/driver/config.h>
#include <ytlib/driver/driver.h>
#include <ytlib/driver/dispatcher.h>

#include <ytlib/formats/format.h>

#include <util/memory/tempbuf.h>

#include <string>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NRpc;
using namespace NYTree;
using namespace NDriver;
using namespace NFormats;
using namespace NConcurrency;

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

// Assuming presence of outer HandleScope.
void Invoke(
    const Handle<Function>& callback,
    const Handle<Value>& a1)
{
    Handle<Value> args[] = { a1 };
    node::MakeCallback(Object::New(), callback, ARRAY_SIZE(args), args);
}

// Assuming presence of outer HandleScope.
void Invoke(
    const Handle<Function>& callback,
    const Handle<Value>& a1,
    const Handle<Value>& a2)
{
    Handle<Value> args[] = { a1, a2 };
    node::MakeCallback(Object::New(), callback, ARRAY_SIZE(args), args);
}

// Assuming presence of outer HandleScope.
void Invoke(
    const Handle<Function>& callback,
    const Handle<Value>& a1,
    const Handle<Value>& a2,
    const Handle<Value>& a3)
{
    Handle<Value> args[] = { a1, a2, a3 };
    node::MakeCallback(Object::New(), callback, ARRAY_SIZE(args), args);
}

class TUVInvoker
    : public IInvoker
{
public:
    explicit TUVInvoker(uv_loop_t* loop)
        : QueueSize(0)
    {
        memset(&AsyncHandle, 0, sizeof(AsyncHandle));
        YCHECK(uv_async_init(loop, &AsyncHandle, &TUVInvoker::Callback) == 0);
        AsyncHandle.data = this;
    }

    ~TUVInvoker()
    {
        uv_close((uv_handle_t*)&AsyncHandle, nullptr);
    }

    virtual void Invoke(const TClosure& callback) override
    {
        Queue.Enqueue(callback);
        AtomicIncrement(QueueSize);

        YCHECK(uv_async_send(&AsyncHandle) == 0);
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual NConcurrency::TThreadId GetThreadId() const = 0;
    {
        return NConcurrency::InvalidThreadId;
    }

    virtual void VerifyAffinity() const override
    {
        YUNREACHABLE();
    }
#endif

private:
    uv_async_t AsyncHandle;

    TClosure Action;
    TLockFreeQueue<TClosure> Queue;
    TAtomic QueueSize;

    static const int ActionsPerTick = 100;

    static void Callback(uv_async_t* handle, int status)
    {
        THREAD_AFFINITY_IS_V8();

        YCHECK(status == 0);
        YCHECK(handle->data);

        reinterpret_cast<TUVInvoker*>(handle->data)->CallbackImpl();
    }

    void CallbackImpl()
    {
        YCHECK(!Action);

        int actionsRan = 0;
        while (Queue.Dequeue(&Action)) {
            AtomicDecrement(QueueSize);

            Action.Run();
            Action.Reset();

            if (++actionsRan >= ActionsPerTick) {
                YCHECK(uv_async_send(&AsyncHandle) == 0);
                break;
            }
        }

        YCHECK(!Action);
    }
};

// uv_default_loop() is a static singleton object, so it is safe to call
// function at the binding time.
TLazyIntrusivePtr<TUVInvoker> DefaultUVInvoker(BIND(
    &New<TUVInvoker, uv_loop_t* const&>,
    uv_default_loop()));

class TResponseParametersConsumer
    : public TForwardingYsonConsumer
{
public:
    TResponseParametersConsumer(const Persistent<Function>& callback)
        : FlushClosure_(BIND(&TResponseParametersConsumer::DoFlush, this))
        , FlushPending_(0)
        , Callback_(callback)
    {
        THREAD_AFFINITY_IS_V8();
    }

    ~TResponseParametersConsumer()
    {
        THREAD_AFFINITY_IS_V8();
    }

    virtual void OnMyKeyedItem(const TStringBuf& keyRef) override
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

        builder->BeginTree();
        Forward(
            builder.get(),
            BIND(&TResponseParametersConsumer::DoSaveBit, this, Stroka(keyRef), Owned(builder.get())));

        builder.release();
    }

    TFuture<void> Flush()
    {
        auto flushFuture = FlushFuture_;
        if (!flushFuture) {
            TGuard<TSpinLock> guard(Lock_);
            if (!FlushFuture_) {
                FlushFuture_ = FlushClosure_.AsyncVia(DefaultUVInvoker.Get()).Run();
            }
            return FlushFuture_;
        }
        return flushFuture;
    }

private:
    typedef std::tuple<Stroka, INodePtr> Bit;

    TSpinLock Lock_;
    std::deque<Bit> Bits_;

    TClosure FlushClosure_;
    TAtomic FlushPending_;
    TFuture<void> FlushFuture_;

    const Persistent<Function>& Callback_;

    void DoFlush()
    {
        THREAD_AFFINITY_IS_V8();
        HandleScope scope;

        std::deque<Bit> bitsToFlush;
        {
            TGuard<TSpinLock> guard(Lock_);
            bitsToFlush.swap(Bits_);
            FlushFuture_.Reset();
        }

        for (const auto& bit : bitsToFlush) {
            auto keyHandle = String::New(std::get<0>(bit).c_str());
            auto valueHandle = ConvertNodeToV8Value(std::get<1>(bit));

            Invoke(Callback_, keyHandle, valueHandle);
        }
    }

    void DoSaveBit(const Stroka& key, ITreeBuilder* builder)
    {
        {
            TGuard<TSpinLock> guard(Lock_);
            Bits_.emplace_back(std::forward_as_tuple(
                std::move(key),
                builder->EndTree()));
        }

        Flush();
    }

};

// TODO(sandello): Refactor this huge mess.
struct TExecuteRequest
{
    uv_work_t Request;
    TDriverWrap* Wrap;

    Persistent<Function> ExecuteCallback;
    Persistent<Function> ParameterCallback;

    TNodeJSInputStack InputStack;
    TNodeJSOutputStack OutputStack;
    TResponseParametersConsumer ResponseParametersConsumer;

    TDriverRequest DriverRequest;
    TDriverResponse DriverResponse;

    NTracing::TTraceContext TraceContext;

    TExecuteRequest(
        TDriverWrap* wrap,
        TInputStreamWrap* inputStream,
        TOutputStreamWrap* outputStream,
        Handle<Function> executeCallback,
        Handle<Function> parameterCallback)
        : Wrap(wrap)
        , ExecuteCallback(Persistent<Function>::New(executeCallback))
        , ParameterCallback(Persistent<Function>::New(parameterCallback))
        , InputStack(inputStream)
        , OutputStack(outputStream)
        , ResponseParametersConsumer(ParameterCallback)
    {
        THREAD_AFFINITY_IS_V8();
        YASSERT(Wrap);

        Wrap->Ref();
    }

    ~TExecuteRequest()
    {
        THREAD_AFFINITY_IS_V8();

        ExecuteCallback.Dispose();
        ExecuteCallback.Clear();

        ParameterCallback.Dispose();
        ParameterCallback.Clear();

        Wrap->Unref();
    }

    TInputStreamWrap* GetNodeJSInputStream()
    {
        return InputStack.GetBaseStream();
    }

    TOutputStreamWrap* GetNodeJSOutputStream()
    {
        return OutputStack.GetBaseStream();
    }

    void SetCommand(
        Stroka commandName,
        Stroka authenticatedUser,
        INodePtr arguments,
        ui64 requestId)
    {
        DriverRequest.CommandName = std::move(commandName);
        DriverRequest.AuthenticatedUser = std::move(authenticatedUser);
        DriverRequest.Arguments = arguments->AsMap();

        auto trace = DriverRequest.Arguments->FindChild("trace");
        if (trace && ConvertTo<bool>(trace)) {
            TraceContext = NTracing::CreateRootTraceContext();
            if (requestId) {
                TraceContext = NTracing::TTraceContext(
                    requestId,
                    TraceContext.GetSpanId(),
                    TraceContext.GetParentSpanId());
            }
        }
    }

    void SetInputCompression(ECompression compression)
    {
        InputStack.AddCompression(compression);
    }

    void SetOutputCompression(ECompression compression)
    {
        OutputStack.AddCompression(compression);
    }

    void Prepare()
    {
        Request.data = this;

        DriverRequest.InputStream = CreateAsyncInputStream(&InputStack);
        DriverRequest.OutputStream = CreateAsyncOutputStream(&OutputStack);
        DriverRequest.ResponseParametersConsumer = &ResponseParametersConsumer;
    }

    void Finish()
    {
        OutputStack.Finish();
    }

    void Await()
    {
        ResponseParametersConsumer.Flush().Get();
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
        Boolean::New(descriptor.IsVolatile),
        v8::ReadOnly);
    result->Set(
        DescriptorIsHeavy,
        Boolean::New(descriptor.IsHeavy),
        v8::ReadOnly);
    return result;
}

// Assuming presence of outer HandleScope.
template <class E>
void ExportEnumeration(
    const Handle<Object>& target,
    const char* name)
{
    auto values = E::GetDomainValues();
    Local<Array> mapping = Array::New();

    for (auto value : values) {
        auto key = Stroka::Join(name, "_", E::GetLiteralByValue(value)->data());
        auto keyHandle = String::NewSymbol(key.c_str());
        auto valueHandle = Integer::New(value);
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
        Message = Format("Error loading configuration\n%v", ex.what());
        return;
    }

    try {
        NDriver::TDispatcher::Get()->Configure(config->Driver->HeavyPoolSize);
        Driver = CreateDriver(config->Driver);
    } catch (const std::exception& ex) {
        Message = Format("Error initializing driver instance\n%v", ex.what());
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

    YASSERT(args.Length() == 2);

    EXPECT_THAT_IS(args[0], Boolean);
    EXPECT_THAT_IS(args[1], Object);

    TDriverWrap* wrap = NULL;
    try {
        wrap = new TDriverWrap(
            args[0]->BooleanValue(),
            args[1].As<Object>());
        wrap->Wrap(args.This());

        if (wrap->Driver) {
            return args.This();
        } else {
            return ThrowException(Exception::Error(String::New(~wrap->Message)));
        }
    } catch (const std::exception& ex) {
        if (wrap) {
            delete wrap;
        }

        return ThrowException(Exception::Error(String::New(ex.what())));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TDriverWrap::FindCommandDescriptor(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap object.
    TDriverWrap* driver =
        ObjectWrap::Unwrap<TDriverWrap>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    // Unwrap arguments.
    String::Utf8Value commandNameValue(args[0]);
    Stroka commandName(*commandNameValue, commandNameValue.length());

    // Do the work.
    return scope.Close(driver->DoFindCommandDescriptor(commandName));
}

Handle<Value> TDriverWrap::DoFindCommandDescriptor(const Stroka& commandName)
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
    TDriverWrap* driver =
        ObjectWrap::Unwrap<TDriverWrap>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

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
    YASSERT(args.Length() == 10);

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
    TDriverWrap* host = ObjectWrap::Unwrap<TDriverWrap>(args.This());

    String::AsciiValue commandName(args[0]);
    String::AsciiValue authenticatedUser(args[1]);

    TInputStreamWrap* inputStream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args[2].As<Object>());
    ECompression inputCompression =
        (ECompression)args[3]->Uint32Value();

    TOutputStreamWrap* outputStream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args[4].As<Object>());
    ECompression outputCompression =
        (ECompression)args[5]->Uint32Value();

    INodePtr parameters = TNodeWrap::UnwrapNode(args[6]);

    ui64 requestId = 0;

    if (node::Buffer::HasInstance(args[7])) {
        const char* buffer = node::Buffer::Data(args[7].As<Object>());
        size_t length = node::Buffer::Length(args[7].As<Object>());
        if (length == 8) {
            requestId = __builtin_bswap64(*(ui64*)buffer);
        }
    }

    Local<Function> executeCallback = args[8].As<Function>();
    Local<Function> parameterCallback = args[9].As<Function>();

    // Build an atom of work.
    YCHECK(parameters);
    YCHECK(parameters->GetType() == ENodeType::Map);

    std::unique_ptr<TExecuteRequest> request(new TExecuteRequest(
        host,
        inputStream,
        outputStream,
        executeCallback,
        parameterCallback));

    try {
        request->SetCommand(
            Stroka(*commandName, commandName.length()),
            Stroka(*authenticatedUser, authenticatedUser.length()),
            std::move(parameters),
            requestId);

        request->SetInputCompression(inputCompression);
        request->SetOutputCompression(outputCompression);

        request->Prepare();

        uv_queue_work(
            uv_default_loop(), &request.release()->Request,
            TDriverWrap::ExecuteWork, TDriverWrap::ExecuteAfter);
    } catch (const std::exception& ex) {
        TError error(ex);
        LOG_DEBUG(error, "Unexpected exception while creating TExecuteRequest");

        Invoke(request->ExecuteCallback, ConvertErrorToV8(error));
    }

    return Undefined();
}

void TDriverWrap::ExecuteWork(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_UV();
    TExecuteRequest* request = container_of(workRequest, TExecuteRequest, Request);

    if (LIKELY(!request->Wrap->Echo)) {
        NTracing::TTraceContextGuard guard(request->TraceContext);

        // Execute() method is guaranteed to be exception-safe,
        // so no try-catch here.
        request->DriverResponse = request->Wrap->Driver->Execute(request->DriverRequest).Get();
        request->Await();
    } else {
        TTempBuf buffer;
        auto inputStream = CreateSyncInputStream(request->DriverRequest.InputStream);
        auto outputStream = CreateSyncOutputStream(request->DriverRequest.OutputStream);

        while (size_t length = inputStream->Load(buffer.Data(), buffer.Size())) {
            outputStream->Write(buffer.Data(), length);
        }

        request->DriverResponse = TDriverResponse();
    }
}

void TDriverWrap::ExecuteAfter(uv_work_t* workRequest)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    std::unique_ptr<TExecuteRequest> request(
        container_of(workRequest, TExecuteRequest, Request));

    try {
        if (LIKELY(request->OutputStack.HasAnyData())) {
            request->Finish();
        } else {
            // In this case we have to prematurely destroy the stream to avoid
            // writing middleware-induced framing overhead.
            request->OutputStack.GetBaseStream()->DoDestroy();
        }
    } catch (const std::exception& ex) {
        LOG_DEBUG(TError(ex), "Ignoring exception while closing driver output stream");
    }

    // XXX(sandello): We cannot represent ui64 precisely in V8, because there
    // is no native ui64 integer type. So we convert ui64 to double (v8::Number)
    // to precisely represent all integers up to 2^52
    // (see http://en.wikipedia.org/wiki/Double_precision).
    double bytesIn = request->InputStack.GetBaseStream()->GetBytesEnqueued();
    double bytesOut = request->OutputStack.GetBaseStream()->GetBytesEnqueued();

    Invoke(
        request->ExecuteCallback,
        ConvertErrorToV8(request->DriverResponse.Error),
        Number::New(bytesIn),
        Number::New(bytesOut));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

