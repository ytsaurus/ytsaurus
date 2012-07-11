#include "stdafx.h"
#include "yson_producer.h"
#include "ytree.h"
#include "yson_parser.h"
#include "yson_stream.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "null_yson_consumer.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NYTree {

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

// i64
void Serialize(i64 value, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(value);
}

// ui64
void Serialize(ui64 value, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(value);
}

// i32
void Serialize(i32 value, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(value);
}

// ui32
void Serialize(ui32 value, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(value);
}

// ui16
void Serialize(ui16 value, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(value);
}

// double
void Serialize(double value, IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(value);
}

// Stroka
void Serialize(const Stroka& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// TStringBuf
void Serialize(const TStringBuf& value, IYsonConsumer* consumer)
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
    consumer->OnStringScalar(FormatBool(value));
}

// char
void Serialize(char value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(Stroka(value));
}

// TDuration
void Serialize(TDuration value, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(value.MilliSeconds());
}

// TInstant
void Serialize(TInstant value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

// TGuid
void Serialize(const TGuid& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

// TInputStream
void Serialize(TInputStream& input, IYsonConsumer* consumer)
{
    Serialize(TYsonInput(&input), consumer);
}

// TNodePtr
void Serialize(INode& value, IYsonConsumer* consumer)
{
    VisitTree(&value, consumer);
}

////////////////////////////////////////////////////////////////////////////////

// i64
void Deserialize(i64& value, INodePtr node)
{
    value = node->AsInteger()->GetValue();
}

// i32
void Deserialize(i32& value, INodePtr node)
{
    value = CheckedStaticCast<i32>(node->AsInteger()->GetValue());
}

// ui32
void Deserialize(ui32& value, INodePtr node)
{
    value = CheckedStaticCast<ui32>(node->AsInteger()->GetValue());
}

// ui16
void Deserialize(ui16& value, INodePtr node)
{
    value = CheckedStaticCast<ui16>(node->AsInteger()->GetValue());
}

// double
void Deserialize(double& value, INodePtr node)
{
    value = node->AsDouble()->GetValue();
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
        ythrow yexception() <<
            Sprintf("Expected string of length 1 but found of length %" PRISZT, stringValue.size());
    }
    value = stringValue[0];
}

// TDuration
void Deserialize(TDuration& value, INodePtr node)
{
    value = TDuration::MilliSeconds(node->AsInteger()->GetValue());
}

// TInstant
void Deserialize(TInstant& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Integer) {
        value = TInstant::MilliSeconds(node->AsInteger()->GetValue());
    } else {
        value = TInstant::ParseIso8601(node->AsString()->GetValue());
    }
}

// TGuid
void Deserialize(TGuid& value, INodePtr node)
{
    value = TGuid::FromString(node->AsString()->GetValue());
}

// TNodePtr
void Deserialize(INodePtr& value, INodePtr node)
{
    value = node;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
