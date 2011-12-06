#include "ytree.h"
#include "yson_reader.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "serialize.h"

namespace NYT {
namespace NYTree {

namespace NDetail {

void ProducerFromYsonThunk(IYsonConsumer* consumer, TInputStream* input)
{
    TYsonReader reader(consumer);
    reader.Read(input);
}

} // NDetail

TYsonProducer::TPtr ProducerFromYson(TInputStream* input)
{
    return FromMethod(&NDetail::ProducerFromYsonThunk, input);
}

INode::TPtr DeserializeFromYson(TInputStream* input, INodeFactory* factory)
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    TYsonReader reader(~builder);
    reader.Read(input);
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


} // namespace NYTree
} // namespace NYT
