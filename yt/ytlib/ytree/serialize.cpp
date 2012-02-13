#include "ytree.h"
#include "yson_reader.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "serialize.h"

#include "../misc/configurable.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer::TPtr ProducerFromYson(TInputStream* input)
{
    return FromFunctor([=] (IYsonConsumer* consumer)
        {
            TYsonReader reader(consumer, input);
            reader.Read();
        });
}

TYsonProducer::TPtr ProducerFromYson(const TYson& data)
{
    return FromFunctor([=] (IYsonConsumer* consumer)
        {
            TStringInput input(data);
            TYsonReader reader(consumer, &input);
            reader.Read();
        });
}

TYsonProducer::TPtr ProducerFromNode(const INode* node)
{
    return FromFunctor([=] (IYsonConsumer* consumer)
        {
            TTreeVisitor visitor(consumer);
            visitor.Visit(node);
        });
}

INode::TPtr DeserializeFromYson(TInputStream* input, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    TYsonReader reader(~builder, input);
    reader.Read();
    return builder->EndTree();
}

INode::TPtr DeserializeFromYson(const TYson& yson, INodeFactory* factory)
{
    TStringInput input(yson);
    return DeserializeFromYson(&input, factory);
}

TOutputStream& SerializeToYson(
    const INode* node,
    TOutputStream& output,
    EYsonFormat format)
{
    TYsonWriter writer(&output, format);
    TTreeVisitor visitor(&writer);
    visitor.Visit(node);
    return output;
}

INode::TPtr CloneNode(const INode* node, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    TTreeVisitor visitor(~builder);
    visitor.Visit(node);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

// i64
void Read(i64& parameter, const INode* node)
{
    parameter = node->AsInt64()->GetValue();
}

// i32
void Read(i32& parameter, const INode* node)
{
    parameter = CheckedStaticCast<i32>(node->AsInt64()->GetValue());
}

// ui32
void Read(ui32& parameter, const INode* node)
{
    parameter = CheckedStaticCast<ui32>(node->AsInt64()->GetValue());
}

// ui16
void Read(ui16& parameter, const INode* node)
{
    parameter = CheckedStaticCast<ui16>(node->AsInt64()->GetValue());
}

// double
void Read(double& parameter, const INode* node)
{
    parameter = node->AsDouble()->GetValue();
}

// Stroka
void Read(Stroka& parameter, const INode* node)
{
    parameter = node->AsString()->GetValue();
}

// bool
void Read(bool& parameter, const INode* node)
{
    Stroka value = node->AsString()->GetValue();
    parameter = ParseBool(value);
}

// TDuration
void Read(TDuration& parameter, const INode* node)
{
    parameter = TDuration::MilliSeconds(node->AsInt64()->GetValue());
}

// TGuid
void Read(TGuid& parameter, const INode* node)
{
    parameter = TGuid::FromString(node->AsString()->GetValue());
}

// INode::TPtr
void Read(
    INode::TPtr& parameter,
    const INode* node)
{
    parameter = const_cast<INode*>(node);
}

////////////////////////////////////////////////////////////////////////////////

// i64
void Write(i64 parameter, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// i32
void Write(i32 parameter, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// ui32
void Write(ui32 parameter, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
}

// ui16
void Write(ui16 parameter, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter);
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
void Write(const TDuration& parameter, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(parameter.MilliSeconds());
}

// TGuid
void Write(const TGuid& parameter, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(parameter.ToString());
}

// INode::TPtr
void Write(const INode& parameter, IYsonConsumer* consumer)
{
    TTreeVisitor visitor(consumer, false);
    visitor.Visit(&parameter);
}

// TYsonProducer
void Write(TYsonProducer& parameter, IYsonConsumer* consumer)
{
    parameter.Do(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
