#include "ytree.h"
#include "yson_reader.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "serialize.h"
#include "null_yson_consumer.h"

#include "../misc/configurable.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer ProducerFromYson(TInputStream* input)
{
    return BIND([=] (IYsonConsumer* consumer) {
        TYsonReader reader(consumer, input);
        reader.Read();
    });
}

TYsonProducer ProducerFromYson(const TYson& data)
{
    return BIND([=] (IYsonConsumer* consumer) {
        TStringInput input(data);
        TYsonReader reader(consumer, &input);
        reader.Read();
    });
}

TYsonProducer ProducerFromNode(INode* node)
{
    return BIND([=] (IYsonConsumer* consumer) {
        VisitTree(node, consumer);
    });
}

void ValidateYson(TInputStream* input)
{
    TYsonReader reader(GetNullYsonConsumer(), input);
    reader.Read();
}

void ValidateYson(const TYson& yson)
{
    TStringInput input(yson);
    TYsonReader reader(GetNullYsonConsumer(), &input);
    reader.Read();
}

INodePtr DeserializeFromYson(TInputStream* input, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    TYsonReader reader(~builder, input);
    reader.Read();
    return builder->EndTree();
}

INodePtr DeserializeFromYson(const TYson& yson, INodeFactory* factory)
{
    TStringInput input(yson);
    return DeserializeFromYson(&input, factory);
}

TOutputStream& SerializeToYson(
    INode* node,
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

INodePtr CloneNode(INode* node, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    VisitTree(node, ~builder);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

// i64
void Read(i64& parameter, INode* node)
{
    parameter = node->AsInteger()->GetValue();
}

// i32
void Read(i32& parameter, INode* node)
{
    parameter = CheckedStaticCast<i32>(node->AsInteger()->GetValue());
}

// ui32
void Read(ui32& parameter, INode* node)
{
    parameter = CheckedStaticCast<ui32>(node->AsInteger()->GetValue());
}

// ui16
void Read(ui16& parameter, INode* node)
{
    parameter = CheckedStaticCast<ui16>(node->AsInteger()->GetValue());
}

// double
void Read(double& parameter, INode* node)
{
    parameter = node->AsDouble()->GetValue();
}

// Stroka
void Read(Stroka& parameter, INode* node)
{
    parameter = node->AsString()->GetValue();
}

// bool
void Read(bool& parameter, INode* node)
{
    Stroka value = node->AsString()->GetValue();
    parameter = ParseBool(value);
}

// TDuration
void Read(TDuration& parameter, INode* node)
{
    parameter = TDuration::MilliSeconds(node->AsInteger()->GetValue());
}

// TGuid
void Read(TGuid& parameter, INode* node)
{
    parameter = TGuid::FromString(node->AsString()->GetValue());
}

// TNodePtr
void Read(
    INodePtr& parameter,
    INode* node)
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
