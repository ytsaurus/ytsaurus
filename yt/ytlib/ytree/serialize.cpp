#include "ytree.h"
#include "yson_reader.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "serialize.h"

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
    TYsonWriter::EFormat format)
{
    TYsonWriter writer(&output, format);
    TTreeVisitor visitor(&writer);
    visitor.Visit(node);
    return output;
}

TYson SerializeToYson(const INode* node, TYsonWriter::EFormat format)
{
    TStringStream output;
    SerializeToYson(node, output, format);
    return output.Str();
}

TYson SerializeToYson(TYsonProducer* producer, TYsonWriter::EFormat format)
{
    TStringStream output;
    TYsonWriter writer(&output, format);
    producer->Do(&writer);
    return output.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
