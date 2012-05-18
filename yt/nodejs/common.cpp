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

void ConsumeV8Array(Handle<Array> array, IYsonConsumer* consumer);
void ConsumeV8Object(Handle<Object> object, IYsonConsumer* consumer);
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

    consumer->OnBeginMap();

    Local<Array> properties = object->GetOwnPropertyNames();
    for (ui32 i = 0; i < properties->Length(); ++i) {
        Local<String> key = properties->Get(i)->ToString();
        String::AsciiValue keyValue(key);

        consumer->OnKeyedItem(TStringBuf(*keyValue, keyValue.length()));
        ConsumeV8Value(object->Get(key), consumer);
    }

    consumer->OnEndMap();
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
    } else if (value->IsBoolean()) {
        if (value->BooleanValue()) {
            consumer->OnStringScalar("true");
        } else {
            consumer->OnStringScalar("false");
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
            "Unable to map JS value onto YSON")));
    }
}

} // namespace

INodePtr ConvertV8ValueToYson(Handle<Value> value)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    ConsumeV8Value(value, ~builder);
    return builder->EndTree();
}

INodePtr ConvertV8StringToYson(Handle<String> string)
{
    String::AsciiValue value(string);
    return DeserializeFromYson(TStringBuf(*value, value.length()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
