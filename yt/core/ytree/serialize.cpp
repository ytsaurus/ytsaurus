#include "stdafx.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

NYson::EYsonType GetYsonType(const TYsonString& yson)
{
    return yson.GetType();
}

NYson::EYsonType GetYsonType(const TYsonInput& input)
{
    return input.GetType();
}

NYson::EYsonType GetYsonType(const TYsonProducer& producer)
{
    return producer.GetType();
}

////////////////////////////////////////////////////////////////////////////////

// integers
#define SERIALIZE(type) \
    void Serialize(type value, NYson::IYsonConsumer* consumer) \
    { \
        consumer->OnInt64Scalar(CheckedStaticCast<i64>(value)); \
    }

SERIALIZE(short)
SERIALIZE(unsigned short)
SERIALIZE(int)
SERIALIZE(unsigned)
SERIALIZE(long)
SERIALIZE(unsigned long)
SERIALIZE(long long)
SERIALIZE(unsigned long long)

#undef SERIALIZE

// double
void Serialize(double value, NYson::IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(value);
}

// Stroka
void Serialize(const Stroka& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// TStringBuf
void Serialize(const TStringBuf& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// const char*
void Serialize(const char* value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(TStringBuf(value));
}

// bool
void Serialize(bool value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(FormatBool(value));
}

// char
void Serialize(char value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(Stroka(value));
}

// TDuration
void Serialize(TDuration value, NYson::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(value.MilliSeconds());
}

// TInstant
void Serialize(TInstant value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

// TGuid
void Serialize(const TGuid& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(ToString(value));
}

// TInputStream
void Serialize(TInputStream& input, NYson::IYsonConsumer* consumer)
{
    Serialize(TYsonInput(&input), consumer);
}

////////////////////////////////////////////////////////////////////////////////

// integers
#define DESERIALIZE(type) \
    void Deserialize(type& value, INodePtr node) \
    { \
        value = CheckedStaticCast<type>(node->AsInt64()->GetValue()); \
    }

DESERIALIZE(short)
DESERIALIZE(unsigned short)
DESERIALIZE(int)
DESERIALIZE(unsigned)
DESERIALIZE(long)
DESERIALIZE(unsigned long)
DESERIALIZE(long long)
DESERIALIZE(unsigned long long)

#undef DESERIALIZE

// double
void Deserialize(double& value, INodePtr node)
{
    // Allow integer nodes to be serialized into doubles.
    if (node->GetType() == ENodeType::Int64) {
        value = node->AsInt64()->GetValue();
    } else {
        value = node->AsDouble()->GetValue();
    }
}

// Stroka
void Deserialize(Stroka& value, INodePtr node)
{
    value = node->AsString()->GetValue();
}

// bool
void Deserialize(bool& value, INodePtr node)
{
    Stroka stringValue = node->AsString()->GetValue();
    value = ParseBool(stringValue);
}

// char
void Deserialize(char& value, INodePtr node)
{
    Stroka stringValue = node->AsString()->GetValue();
    if (stringValue.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected string of length 1 but found of length %" PRISZT, stringValue.size());
    }
    value = stringValue[0];
}

// TDuration
void Deserialize(TDuration& value, INodePtr node)
{
    value = TDuration::MilliSeconds(node->AsInt64()->GetValue());
}

// TInstant
void Deserialize(TInstant& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Int64) {
        value = TInstant::MilliSeconds(node->AsInt64()->GetValue());
    } else {
        value = TInstant::ParseIso8601(node->AsString()->GetValue());
    }
}

// TGuid
void Deserialize(TGuid& value, INodePtr node)
{
    value = TGuid::FromString(node->AsString()->GetValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
