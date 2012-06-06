#include "common.h"

#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void DoNothing(uv_work_t* request)
{ }

////////////////////////////////////////////////////////////////////////////////

namespace {

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

    auto maybeValue = object->Get(SpecialValueKey);
    auto maybeAttributes = object->Get(SpecialAttributesKey);

    if (maybeValue) {
        if (maybeAttributes) {
            if (!maybeAttributes->IsObject()) {
                ThrowException(Exception::TypeError(String::New(
                    "Attributes have to be an object")));
                return;
            }

            consumer->OnBeginAttributes();
            ConsumeV8ObjectProperties(maybeAttributes->ToObject(), consumer);
            consumer->OnEndAttributes();
        }

        ConsumeV8Value(maybeValue);
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
        ThrowException(Exception::TypeError(String::New(
            "Unable to map V8 value onto YSON; unsupported value type")));
        return;
    }
}

} // namespace

INodePtr ConvertV8ValueToYson(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;
    TryCatch block;

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    ConsumeV8Value(value, ~builder);

    if (block.HasCaught()) {
        block.ReThrow();
        return NULL;
    } else {
        return builder->EndTree();
    }
}

INodePtr ConvertV8StringToYson(Handle<String> string)
{
    String::AsciiValue value(string);
    return DeserializeFromYson(TStringBuf(*value, value.length()));
}

void Initialize(Handle<Object> target)
{
    SpecialValueKey = NODE_PSYMBOL("$value");
    SpecialAttributesKey = NODE_PSYMBOL("$attributes");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
