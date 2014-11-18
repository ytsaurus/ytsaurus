#include "node.h"
#include "error.h"
#include "stream_stack.h"

#include <core/ytree/node.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/yson_string.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/convert.h>
#include <core/ytree/ypath_client.h>

#include <core/yson/consumer.h>
#include <core/yson/writer.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/utf8_decoder.h>

#include <util/stream/zlib.h>
#include <util/stream/lz.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NYson;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

namespace {

static Persistent<String> SpecialValueKey;
static Persistent<String> SpecialAttributesKey;

// Declare.
void ConsumeV8Array(Handle<Array> array, ITreeBuilder* builder);
void ConsumeV8Object(Handle<Object> object, ITreeBuilder* builder);
void ConsumeV8ObjectProperties(Handle<Object> object, ITreeBuilder* builder);
void ConsumeV8Value(Handle<Value> value, ITreeBuilder* builder);

Handle<Value> ProduceV8(const INodePtr& node);

// Define.
void ConsumeV8Array(Handle<Array> array, ITreeBuilder* builder)
{
    THREAD_AFFINITY_IS_V8();

    builder->OnBeginList();

    for (ui32 i = 0; i < array->Length(); ++i) {
        builder->OnListItem();
        ConsumeV8Value(array->Get(i), builder);
    }

    builder->OnEndList();
}

void ConsumeV8Object(Handle<Object> object, ITreeBuilder* builder)
{
    THREAD_AFFINITY_IS_V8();

    if (object->Has(SpecialValueKey)) {
        auto value = object->Get(SpecialValueKey);
        if (object->Has(SpecialAttributesKey)) {
            auto attributes = object->Get(SpecialAttributesKey);
            if (!attributes->IsObject()) {
                THROW_ERROR_EXCEPTION("Attributes have to be a V8 object");
                return;
            }

            builder->OnBeginAttributes();
            ConsumeV8ObjectProperties(attributes->ToObject(), builder);
            builder->OnEndAttributes();
        }

        ConsumeV8Value(value, builder);
    } else {
        builder->OnBeginMap();
        ConsumeV8ObjectProperties(object, builder);
        builder->OnEndMap();
    }
}

void ConsumeV8ObjectProperties(Handle<Object> object, ITreeBuilder* builder)
{
    THREAD_AFFINITY_IS_V8();

    Local<Array> properties = object->GetOwnPropertyNames();
    for (ui32 i = 0; i < properties->Length(); ++i) {
        Local<String> key = properties->Get(i)->ToString();
        String::AsciiValue keyValue(key);

        builder->OnKeyedItem(TStringBuf(*keyValue, keyValue.length()));
        ConsumeV8Value(object->Get(key), builder);
    }
}

void ConsumeV8Value(Handle<Value> value, ITreeBuilder* builder)
{
    THREAD_AFFINITY_IS_V8();

    /****/ if (value->IsString()) {
        String::Utf8Value utf8Value(value->ToString());
        TStringBuf string(*utf8Value, utf8Value.length());

        TUtf8Transcoder utf8Decoder;
        builder->OnStringScalar(utf8Decoder.Decode(string));
    } else if (value->IsNumber()) {
        if (value->IsInt32()) {
            builder->OnInt64Scalar(value->IntegerValue());
        } else if (value->IsUint32()) {
            builder->OnUint64Scalar(value->IntegerValue());
        } else {
            builder->OnDoubleScalar(value->NumberValue());
        }
    } else if (value->IsObject()) {
        if (TNodeWrap::HasInstance(value)) {
            builder->OnNode(CloneNode(TNodeWrap::UnwrapNode(value)));
            return;
        }
        if (value->IsArray()) {
            ConsumeV8Array(
                Local<Array>::Cast(Local<Value>::New(value)),
                builder);
        } else {
            ConsumeV8Object(
                Local<Object>::Cast(Local<Value>::New(value)),
                builder);
        }
    } else if (value->IsBoolean()) {
        builder->OnBooleanScalar(value->BooleanValue());
    } else {
        String::AsciiValue asciiValue(value);
        THROW_ERROR_EXCEPTION(
            "Unsupported JS value type within V8-to-YSON conversion: %s",
            *asciiValue);
    }
}

Handle<Value> ProduceV8(const INodePtr& node)
{
    THREAD_AFFINITY_IS_V8();

    if (!node) {
        return v8::Undefined();
    }

    switch (node->GetType()) {
        case ENodeType::String: {
            auto value = node->GetValue<Stroka>();
            return String::New(value.c_str(), value.length());
        }
        case ENodeType::Int64: {
            return Integer::New(node->GetValue<i64>());
        }
        case ENodeType::Uint64: {
            return Integer::NewFromUnsigned(node->GetValue<ui64>());
        }
        case ENodeType::Double: {
            return Number::New(node->GetValue<double>());
        }
        case ENodeType::Boolean: {
            return Boolean::New(node->GetValue<bool>());
        }
        case ENodeType::Map: {
            auto children = node->AsMap()->GetChildren();
            auto result = Object::New();
            for (const auto& pair : children) {
                const auto& key = pair.first;
                const auto& value = pair.second;
                result->Set(
                    String::New(key.c_str(), key.length()),
                    ProduceV8(value));
            }
            return result;
        }
        case ENodeType::List: {
            auto children = node->AsList()->GetChildren();
            auto result = Array::New(children.size());
            for (size_t i = 0; i < children.size(); ++i) {
                result->Set(
                    Integer::New(i),
                    ProduceV8(children[i]));
            }
            return result;
        }
        default: {
            return v8::Null();
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertV8ValueToNode(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    ConsumeV8Value(value, builder.get());
    return builder->EndTree();
}

Handle<Value> ConvertNodeToV8Value(const INodePtr& node)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return scope.Close(ProduceV8(node));
}

INodePtr ConvertBytesToNode(
    const char* buffer,
    size_t length,
    ECompression compression,
    INodePtr format)
{
    TMemoryInput baseStream(buffer, length);
    TGrowingInputStreamStack streamStack(&baseStream);
    AddCompressionToStack(streamStack, compression);

    return ConvertToNode(CreateProducerForFormat(
        ConvertTo<TFormat>(std::move(format)),
        EDataType::Structured,
        streamStack.Top()));
}

Stroka ConvertNodeToBytes(
    INodePtr node,
    ECompression compression,
    INodePtr format)
{
    Stroka result;
    TStringOutput baseStream(result);
    TGrowingOutputStreamStack streamStack(&baseStream);
    AddCompressionToStack(streamStack, compression);

    auto consumer = CreateConsumerForFormat(
        ConvertTo<TFormat>(std::move(format)),
        EDataType::Structured,
        streamStack.Top());
    Serialize(std::move(node), consumer.get());
    streamStack.Top()->Flush();
    streamStack.Top()->Finish();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TNodeWrap::ConstructorTemplate;

TNodeWrap::TNodeWrap(INodePtr node)
    : node::ObjectWrap()
    , Node_(std::move(node))
{
    THREAD_AFFINITY_IS_V8();
}

TNodeWrap::~TNodeWrap() throw()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    SpecialValueKey = NODE_PSYMBOL("$value");
    SpecialAttributesKey = NODE_PSYMBOL("$attributes");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TNodeWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TNodeWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Print", TNodeWrap::Print);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Get", TNodeWrap::Get);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "GetByYPath", TNodeWrap::GetByYPath);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "SetByYPath", TNodeWrap::SetByYPath);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "GetAttribute", TNodeWrap::GetAttribute);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "SetAttribute", TNodeWrap::SetAttribute);

    target->Set(
        String::NewSymbol("TNodeWrap"),
        ConstructorTemplate->GetFunction());
    target->Set(
        String::NewSymbol("CreateMergedNode"),
        FunctionTemplate::New(TNodeWrap::CreateMerged)->GetFunction());
    target->Set(
        String::NewSymbol("CreateV8Node"),
        FunctionTemplate::New(TNodeWrap::CreateV8)->GetFunction());
}

bool TNodeWrap::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

INodePtr TNodeWrap::UnwrapNode(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return ObjectWrap::Unwrap<TNodeWrap>(value->ToObject())->GetNode();
}

Handle<Value> TNodeWrap::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    try {
        INodePtr node;

        /****/ if (args.Length() == 0) {
            node = nullptr;
        } else if (args.Length() == 1) {
            auto arg = args[0];
            if (arg->IsObject()) {
                node = ConvertV8ValueToNode(arg);
            } else if (arg->IsString()) {
                String::Utf8Value argValue(arg->ToString());
                node = ConvertToNode(TYsonString(Stroka(*argValue, argValue.length())));
            } else if (arg->IsNull() || arg->IsUndefined()) {
                node = nullptr;
            } else {
                THROW_ERROR_EXCEPTION(
                    "1-ary constructor of TNodeWrap can consume either Object or String or Null or Undefined");
            }
        } else if (args.Length() == 3) {
            EXPECT_THAT_IS(args[1], Uint32);
            EXPECT_THAT_HAS_INSTANCE(args[2], TNodeWrap);

            ECompression compression = (ECompression)args[1]->Uint32Value();
            INodePtr format = TNodeWrap::UnwrapNode(args[2]);

            auto arg = args[0];
            if (node::Buffer::HasInstance(arg)) {
                node = ConvertBytesToNode(
                    node::Buffer::Data(arg->ToObject()),
                    node::Buffer::Length(arg->ToObject()),
                    compression,
                    format);
            } else if (arg->IsString()) {
                String::Utf8Value argValue(arg->ToString());
                node = ConvertBytesToNode(
                    *argValue,
                    argValue.length(),
                    compression,
                    format);
            } else {
                THROW_ERROR_EXCEPTION(
                    "3-ary constructor of TNodeWrap can consume either String or Buffer with compression (Uint32) and format (TNodeWrap)");
            }
        } else {
            THROW_ERROR_EXCEPTION(
                "There are only 0-ary, 1-ary and 3-ary constructors of TNodeWrap");
        }

        std::unique_ptr<TNodeWrap> wrappedNode(new TNodeWrap(node));
        wrappedNode.release()->Wrap(args.This());

        return args.This();
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeWrap::CreateMerged(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    INodePtr delta;
    INodePtr result;

    try {
        for (int i = 0; i < args.Length(); ++i) {
            const auto& arg = args[i];
            if (arg->IsNull() || arg->IsUndefined()) {
                continue;
            } else {
                EXPECT_THAT_HAS_INSTANCE(arg, TNodeWrap);
            }

            delta = TNodeWrap::UnwrapNode(args[i]);
            result = result
                ? UpdateNode(std::move(result), std::move(delta))
                : std::move(delta);
        }
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }

    Local<Object> handle = ConstructorTemplate->GetFunction()->NewInstance();
    ObjectWrap::Unwrap<TNodeWrap>(handle)->SetNode(std::move(result));

    return scope.Close(std::move(handle));
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeWrap::CreateV8(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);

    INodePtr node;

    try {
        node = ConvertV8ValueToNode(args[0]);
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }

    Local<Object> handle = ConstructorTemplate->GetFunction()->NewInstance();
    ObjectWrap::Unwrap<TNodeWrap>(handle)->SetNode(std::move(node));

    return scope.Close(std::move(handle));
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeWrap::Print(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 0 || args.Length() == 2);

    INodePtr node = TNodeWrap::UnwrapNode(args.This());
    Handle<Value> handle = Undefined();

    if (args.Length() == 0) {
        auto string = ConvertToYsonString(node, EYsonFormat::Text);
        handle = String::New(string.Data().c_str(), string.Data().length());
    } else if (args.Length() == 2) {
        EXPECT_THAT_IS(args[0], Uint32);
        EXPECT_THAT_HAS_INSTANCE(args[1], TNodeWrap);

        ECompression compression = (ECompression)args[0]->Uint32Value();
        INodePtr format = TNodeWrap::UnwrapNode(args[1]);

        auto result = ConvertNodeToBytes(
            std::move(node),
            compression,
            std::move(format));
        handle = String::New(result.c_str(), result.length());
    }

    return scope.Close(std::move(handle));
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeWrap::Get(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 0);

    INodePtr node = TNodeWrap::UnwrapNode(args.This());
    return scope.Close(ProduceV8(node));
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeWrap::GetByYPath(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    INodePtr node = TNodeWrap::UnwrapNode(args.This());
    String::AsciiValue pathValue(args[0]->ToString());
    TStringBuf path(*pathValue, pathValue.length());

    try {
        node = GetNodeByYPath(std::move(node), Stroka(path));
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }

    Local<Object> handle = ConstructorTemplate->GetFunction()->NewInstance();
    ObjectWrap::Unwrap<TNodeWrap>(handle)->SetNode(std::move(node));

    return scope.Close(std::move(handle));
}

Handle<Value> TNodeWrap::SetByYPath(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 2);

    EXPECT_THAT_IS(args[0], String);
    EXPECT_THAT_HAS_INSTANCE(args[1], TNodeWrap);

    INodePtr node = TNodeWrap::UnwrapNode(args.This());
    String::AsciiValue pathValue(args[0]->ToString());
    TStringBuf path(*pathValue, pathValue.length());
    INodePtr value = TNodeWrap::UnwrapNode(args[1]);

    try {
        SetNodeByYPath(std::move(node), Stroka(path), std::move(value));
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }

    return args.This();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeWrap::GetAttribute(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    INodePtr node = TNodeWrap::UnwrapNode(args.This());
    String::AsciiValue keyValue(args[0]->ToString());
    TStringBuf key(*keyValue, keyValue.length());

    try {
        node = node->Attributes().Get<INodePtr>(Stroka(key));
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }

    Local<Object> handle = ConstructorTemplate->GetFunction()->NewInstance();
    ObjectWrap::Unwrap<TNodeWrap>(handle)->SetNode(std::move(node));

    return scope.Close(std::move(handle));
}

Handle<Value> TNodeWrap::SetAttribute(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 2);

    EXPECT_THAT_IS(args[0], String);
    EXPECT_THAT_HAS_INSTANCE(args[1], TNodeWrap);

    INodePtr node = TNodeWrap::UnwrapNode(args.This());
    String::AsciiValue keyValue(args[0]->ToString());
    TStringBuf key(*keyValue, keyValue.length());
    INodePtr value = TNodeWrap::UnwrapNode(args[1]);

    try {
        node->MutableAttributes()->Set(Stroka(key), value);
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }

    return args.This();
}


////////////////////////////////////////////////////////////////////////////////

INodePtr TNodeWrap::GetNode()
{
    return Node_;
}

const INodePtr TNodeWrap::GetNode() const
{
    return Node_;
}

void TNodeWrap::SetNode(INodePtr node)
{
    Node_ = std::move(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
