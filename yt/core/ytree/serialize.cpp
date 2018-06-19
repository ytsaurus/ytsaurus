#include "serialize.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/misc/cast.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NYTree {

using namespace NYson;
using namespace google::protobuf;
using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

EYsonType GetYsonType(const TYsonString& yson)
{
    return yson.GetType();
}

EYsonType GetYsonType(const TYsonInput& input)
{
    return input.GetType();
}

EYsonType GetYsonType(const TYsonProducer& producer)
{
    return producer.GetType();
}

////////////////////////////////////////////////////////////////////////////////

// signed integers
#define SERIALIZE(type) \
    void Serialize(type value, IYsonConsumer* consumer) \
    { \
        consumer->OnInt64Scalar(CheckedIntegralCast<i64>(value)); \
    }

SERIALIZE(signed char)
SERIALIZE(short)
SERIALIZE(int)
SERIALIZE(long)
SERIALIZE(long long)


#undef SERIALIZE


// unsigned integers
#define SERIALIZE(type) \
    void Serialize(type value, IYsonConsumer* consumer) \
    { \
        consumer->OnUint64Scalar(CheckedIntegralCast<ui64>(value)); \
    }

SERIALIZE(unsigned char)
SERIALIZE(unsigned short)
SERIALIZE(unsigned)
SERIALIZE(unsigned long)
SERIALIZE(unsigned long long)

#undef SERIALIZE

// double
void Serialize(double value, IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(value);
}

// TString
void Serialize(const TString& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// TStringBuf
void Serialize(TStringBuf value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// const char*
void Serialize(const char* value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(TStringBuf(value));
}

// bool
void Serialize(bool value, IYsonConsumer* consumer)
{
    consumer->OnBooleanScalar(value);
}

// char
void Serialize(char value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(TString(value));
}

// TDuration
void Serialize(TDuration value, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(value.MilliSeconds());
}

// TInstant
void Serialize(TInstant value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

// TGuid
void Serialize(const TGuid& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(ToString(value));
}

// IInputStream
void Serialize(IInputStream& input, IYsonConsumer* consumer)
{
    Serialize(TYsonInput(&input), consumer);
}

// Subtypes of google::protobuf::Message
void SerializeProtobufMessage(
    const Message& message,
    const TProtobufMessageType* type,
    NYson::IYsonConsumer* consumer)
{
    auto byteSize = message.ByteSize();
    std::vector<char> wireBytes;
    wireBytes.reserve(byteSize);
    YCHECK(message.SerializePartialToArray(wireBytes.data(), byteSize));
    ArrayInputStream inputStream(wireBytes.data(), byteSize);
    ParseProtobuf(consumer, &inputStream, type);
}

////////////////////////////////////////////////////////////////////////////////

// signed integers
#define DESERIALIZE(type) \
    void Deserialize(type& value, INodePtr node) \
    { \
        if (node->GetType() == ENodeType::Int64) { \
            value = CheckedIntegralCast<type>(node->AsInt64()->GetValue()); \
        } else if (node->GetType() == ENodeType::Uint64) { \
            value = CheckedIntegralCast<type>(node->AsUint64()->GetValue()); \
        } else { \
            THROW_ERROR_EXCEPTION("Cannot parse \"" #type "\" value from %Qlv", \
                node->GetType()); \
        } \
    }

DESERIALIZE(signed char)
DESERIALIZE(short)
DESERIALIZE(int)
DESERIALIZE(long)
DESERIALIZE(long long)
DESERIALIZE(unsigned char)
DESERIALIZE(unsigned short)
DESERIALIZE(unsigned)
DESERIALIZE(unsigned long)
DESERIALIZE(unsigned long long)

#undef DESERIALIZE

// double
void Deserialize(double& value, INodePtr node)
{
    // Allow integer nodes to be serialized into doubles.
    if (node->GetType() == ENodeType::Int64) {
        value = node->AsInt64()->GetValue();
    } else if (node->GetType() == ENodeType::Uint64) {
        value = node->AsUint64()->GetValue();
    } else {
        value = node->AsDouble()->GetValue();
    }
}

// TString
void Deserialize(TString& value, INodePtr node)
{
    value = node->AsString()->GetValue();
}

// bool
void Deserialize(bool& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Boolean) {
        value = node->GetValue<bool>();
    } else {
        auto stringValue = node->AsString()->GetValue();
        value = ParseBool(stringValue);
    }
}

// char
void Deserialize(char& value, INodePtr node)
{
    TString stringValue = node->AsString()->GetValue();
    if (stringValue.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected string of length 1 but found of length %v", stringValue.size());
    }
    value = stringValue[0];
}

// TDuration
void Deserialize(TDuration& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Int64) {
        value = TDuration::MilliSeconds(node->AsInt64()->GetValue());
    } else {
        value = TDuration::MilliSeconds(node->AsUint64()->GetValue());
    }
}

// TInstant
void Deserialize(TInstant& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Int64) {
        value = TInstant::MilliSeconds(node->AsInt64()->GetValue());
    } else if (node->GetType() == ENodeType::Uint64) {
        value = TInstant::MilliSeconds(node->AsUint64()->GetValue());
    } else {
#ifdef YT_IN_ARCADIA
        value = TInstant::ParseIso8601Deprecated(node->AsString()->GetValue());
#else
        value = TInstant::ParseIso8601(node->AsString()->GetValue());
#endif
    }
}

// TGuid
void Deserialize(TGuid& value, INodePtr node)
{
    value = TGuid::FromString(node->AsString()->GetValue());
}

// Subtypes of google::protobuf::Message
void DeserializeProtobufMessage(
    Message& message,
    const TProtobufMessageType* type,
    const INodePtr& node)
{
    TString wireBytes;
    StringOutputStream outputStream(&wireBytes);
    auto protobufWriter = CreateProtobufWriter(&outputStream, type);
    VisitTree(node, protobufWriter.get());
    if (!message.ParseFromArray(wireBytes.data(), wireBytes.size())) {
        THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
            message.GetTypeName());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
