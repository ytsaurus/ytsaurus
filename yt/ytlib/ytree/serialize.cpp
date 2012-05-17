#include "stdafx.h"
#include "serialize.h"
#include "ytree.h"
#include "yson_parser.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "null_yson_consumer.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer ProducerFromYson(TInputStream* input, EYsonType type)
{
    return BIND([=] (IYsonConsumer* consumer) {
        ParseYson(input, consumer, type);
    });
}

TYsonProducer ProducerFromYson(const TYson& data, EYsonType type)
{
    return BIND([=] (IYsonConsumer* consumer) {
        ParseYson(data, consumer, type);
    });
}

TYsonProducer ProducerFromNode(INodePtr node)
{
    return BIND([=] (IYsonConsumer* consumer) {
        VisitTree(node, consumer);
    });
}

void ValidateYson(TInputStream* input)
{
    ParseYson(input, GetNullYsonConsumer());
}

void ValidateYson(const TYson& yson)
{
    ParseYson(yson, GetNullYsonConsumer());
}

INodePtr DeserializeFromYson(TInputStream* input, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    ParseYson(input, ~builder);
    return builder->EndTree();
}

INodePtr DeserializeFromYson(const TYson& yson, INodeFactory* factory)
{
    TStringInput input(yson);
    return DeserializeFromYson(&input, factory);
}

TOutputStream& SerializeToYson(
    INodePtr node,
    TOutputStream& output,
    EYsonFormat format)
{
    TYsonWriter writer(&output, format);
    VisitTree(node, &writer);
    return output;
}

TYson SerializeToYson(
    TYsonProducer producer,
    EYsonFormat format)
{
    TStringStream output;
    TYsonWriter writer(&output, format);
    producer.Run(&writer);
    return output.Str();
}

INodePtr CloneNode(INodePtr node, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    VisitTree(node, ~builder);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

// i64
void Read(i64& parameter, INodePtr node)
{
    parameter = node->AsInteger()->GetValue();
}

// i32
void Read(i32& parameter, INodePtr node)
{
    parameter = CheckedStaticCast<i32>(node->AsInteger()->GetValue());
}

// ui32
void Read(ui32& parameter, INodePtr node)
{
    parameter = CheckedStaticCast<ui32>(node->AsInteger()->GetValue());
}

// ui16
void Read(ui16& parameter, INodePtr node)
{
    parameter = CheckedStaticCast<ui16>(node->AsInteger()->GetValue());
}

// double
void Read(double& parameter, INodePtr node)
{
    parameter = node->AsDouble()->GetValue();
}

// Stroka
void Read(Stroka& parameter, INodePtr node)
{
    parameter = node->AsString()->GetValue();
}

// bool
void Read(bool& parameter, INodePtr node)
{
    Stroka value = node->AsString()->GetValue();
    parameter = ParseBool(value);
}

// TDuration
void Read(TDuration& parameter, INodePtr node)
{
    parameter = TDuration::MilliSeconds(node->AsInteger()->GetValue());
}

// TInstant
void Read(TInstant& parameter, INodePtr node)
{
    parameter = TInstant::MilliSeconds(node->AsInteger()->GetValue());
}

// TGuid
void Read(TGuid& parameter, INodePtr node)
{
    parameter = TGuid::FromString(node->AsString()->GetValue());
}

// TNodePtr
void Read(INodePtr& parameter, INodePtr node)
{
    parameter = node;
}

////////////////////////////////////////////////////////////////////////////////

// i64
void Write(i64 parameter, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(parameter);
}

// i32
void Write(i32 parameter, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(parameter);
}

// ui32
void Write(ui32 parameter, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(parameter);
}

// ui16
void Write(ui16 parameter, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(parameter);
}

// double
void Write(double parameter, IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(parameter);
}

// Stroka
void Write(const Stroka& parameter, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(parameter);
}

// bool
void Write(bool parameter, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(FormatBool(parameter));
}

// TDuration
void Write(TDuration parameter, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(parameter.MilliSeconds());
}

// TInstant
void Write(TInstant parameter, IYsonConsumer* consumer)
{
    consumer->OnIntegerScalar(parameter.MilliSeconds());
}

// TGuid
void Write(const TGuid& parameter, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(parameter.ToString());
}

// TNodePtr
void Write(INode& parameter, IYsonConsumer* consumer)
{
    VisitTree(&parameter, consumer);
}

// TYsonProducer
void Write(TYsonProducer parameter, IYsonConsumer* consumer)
{
    parameter.Run(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
