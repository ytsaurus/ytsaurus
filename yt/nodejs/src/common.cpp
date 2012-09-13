#include "common.h"

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/convert.h>

extern "C" {
    // XXX(sandello): This is extern declaration of eio's internal functions.
    // -lrt will dynamically bind these symbols. We do this dirty-dirty stuff
    // because we would like to alter the thread pool size.

    extern void eio_set_min_parallel (unsigned int nthreads);
    extern void eio_set_max_parallel (unsigned int nthreads);

    extern unsigned int eio_nreqs    (void); /* number of requests in-flight */
    extern unsigned int eio_nready   (void); /* number of not-yet handled requests */
    extern unsigned int eio_npending (void); /* number of finished but unhandled requests */
    extern unsigned int eio_nthreads (void); /* number of worker threads in use currently */
}

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////
// Stuff related to V8 <> YSON conversions.

static Persistent<String> SpecialValueKey;
static Persistent<String> SpecialAttributesKey;

static const char SpecialBase64Marker = '&';

// TODO(sandello): Support proper Base64 string encoding for YSON strings.

void ConsumeV8Array(Handle<Array> array, IYsonConsumer* consumer);
void ConsumeV8Object(Handle<Object> object, IYsonConsumer* consumer);
void ConsumeV8ObjectProperties(Handle<Object> object, IYsonConsumer* consumer);
void ConsumeV8Value(Handle<Value> value, IYsonConsumer* consumer);

void ConsumeV8Array(Handle<Array> array, IYsonConsumer* consumer)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    consumer->OnBeginList();

    for (ui32 i = 0; i < array->Length(); ++i) {
        consumer->OnListItem();
        ConsumeV8Value(array->Get(i), consumer);
    }

    consumer->OnEndList();
}

void ConsumeV8Object(Handle<Object> object, IYsonConsumer* consumer)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    if (object->Has(SpecialValueKey)) {
        auto value = object->Get(SpecialValueKey);
        if (object->Has(SpecialAttributesKey)) {
            auto attributes = object->Get(SpecialAttributesKey);
            if (!attributes->IsObject()) {
                ythrow yexception() << "Attributes have to be an object";
                return;
            }

            consumer->OnBeginAttributes();
            ConsumeV8ObjectProperties(attributes->ToObject(), consumer);
            consumer->OnEndAttributes();
        }

        ConsumeV8Value(value, consumer);
    } else {
        consumer->OnBeginMap();
        ConsumeV8ObjectProperties(object, consumer);
        consumer->OnEndMap();
    }
}

void ConsumeV8ObjectProperties(Handle<Object> object, IYsonConsumer* consumer)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<Array> properties = object->GetOwnPropertyNames();
    for (ui32 i = 0; i < properties->Length(); ++i) {
        Local<String> key = properties->Get(i)->ToString();
        String::AsciiValue keyValue(key);

        consumer->OnKeyedItem(TStringBuf(*keyValue, keyValue.length()));
        ConsumeV8Value(object->Get(key), consumer);
    }
}

void ConsumeV8Value(Handle<Value> value, IYsonConsumer* consumer)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    /****/ if (value->IsString()) {
        String::AsciiValue string(value->ToString());
        consumer->OnStringScalar(TStringBuf(*string, string.length()));
    } else if (value->IsNumber()) {
        if (value->IsInt32() || value->IsUint32()) {
            consumer->OnIntegerScalar(value->IntegerValue());
        } else {
            consumer->OnDoubleScalar(value->NumberValue());
        }
    } else if (value->IsObject()) {
        if (value->IsArray()) {
            ConsumeV8Array(
                Local<Array>::Cast(Local<Value>::New(value)),
                consumer);
        } else {
            ConsumeV8Object(
                Local<Object>::Cast(Local<Value>::New(value)),
                consumer);
        }
    } else {
        ythrow yexception() << "Unsupported value type";
    }
}

Handle<Value> GetYsonRepresentation(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;
    TryCatch block;

    YASSERT(args.Length() == 1);

    auto ysonNode = ConvertV8ValueToNode(args[0]);
    if (!ysonNode) {
        if (!block.HasCaught()) {
            return scope.Close(String::New("Mysterious failure :("));
        } else {
            return scope.Close(block.ReThrow());
        }
    }

    auto ysonString = ConvertToYsonString(ysonNode, EYsonFormat::Text);
    return scope.Close(String::New(ysonString.Data().c_str()));
}

////////////////////////////////////////////////////////////////////////////////
// Stuff related to EIO

Handle<Value> GetEioInformation(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 0);

    Local<Object> result = Object::New();
    result->Set(String::New("nreqs"),    Integer::NewFromUnsigned(eio_nreqs()));
    result->Set(String::New("nready"),   Integer::NewFromUnsigned(eio_nready()));
    result->Set(String::New("npending"), Integer::NewFromUnsigned(eio_npending()));
    result->Set(String::New("nthreads"), Integer::NewFromUnsigned(eio_nthreads()));
    return scope.Close(result);   
}

Handle<Value> SetEioConcurrency(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], Uint32);

    unsigned int numberOfThreads = args[0]->Uint32Value();

    YCHECK(numberOfThreads > 0);

    eio_set_min_parallel(numberOfThreads);
    eio_set_max_parallel(numberOfThreads);

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////
// Stuff related to global subsystems

Handle<Value> ConfigureSingletons(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], Object);

    INodePtr configNode = ConvertV8ValueToNode(args[0]);
    if (!configNode) {
        Message = "Error converting from V8 to YSON";
        return;
    }

    NNodeJS::THttpProxyConfigPtr config;
    try {
        // Qualify namespace to avoid collision with class method New().
        config = NYT::New<NYT::NNodeJS::THttpProxyConfigPtr>();
        config->Load(configNode);
    } catch (const std::exception& ex) {
        return ThrowException(Exception::TypeError(
            String::Concat(
                String::New("Error loading configuration: "),
                String::New(ex.what()))));
    }

    try {
        NLog::TLogManager::Get()->Configure(config->Logging);
        NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
    } catch (const std::exception& ex) {
        return ThrowException(Exception::TypeError(
            String::Concat(
                String::New("Error initializing driver instance: "),
                String::New(ex.what()))));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertV8ValueToNode(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    try {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        ConsumeV8Value(value, ~builder);
        return builder->EndTree();
    } catch(const std::exception& ex) {
        ThrowException(Exception::TypeError(
            String::Concat(
                String::New("Unable to map V8 value onto YSON: "),
                String::New(ex.what()))));
        return NULL;
    }
}

INodePtr ConvertV8StringToNode(Handle<String> string)
{
    String::AsciiValue value(string);
    return ConvertToNode(TYsonString(Stroka(*value, value.length())));
}

void Initialize(Handle<Object> target)
{
    SpecialValueKey = NODE_PSYMBOL("$value");
    SpecialAttributesKey = NODE_PSYMBOL("$attributes");

    target->Set(
        String::NewSymbol("GetYsonRepresentation"),
        FunctionTemplate::New(GetYsonRepresentation)->GetFunction());
    target->Set(
        String::NewSymbol("GetEioInformation"),
        FunctionTemplate::New(GetEioInformation)->GetFunction());
    target->Set(
        String::NewSymbol("SetEioConcurrency"),
        FunctionTemplate::New(SetEioConcurrency)->GetFunction());
    target->Set(
        String::NewSymbol("ConfigureSingletons"),
        FunctionTemplate::New(ConfigureSingletons)->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
