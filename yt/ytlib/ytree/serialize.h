#pragma once

#include "ephemeral.h"
#include "ytree.h"
#include "yson_reader.h"
#include "yson_writer.h"
#include "tree_visitor.h"
#include "tree_builder.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

inline INode::TPtr DeserializeFromYson(
    TInputStream* input,
    INodeFactory* factory = GetEphemeralNodeFactory())
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    TYsonReader reader(~builder);
    reader.Read(input);
    return builder->EndTree();
}

inline INode::TPtr DeserializeFromYson(
    const TYson& yson,
    INodeFactory* factory = GetEphemeralNodeFactory())
{
    TStringInput input(yson);
    return DeserializeFromYson(&input, factory);
}

inline TOutputStream& SerializeToYson(
    const INode* node,
    TOutputStream& output,
    TYsonWriter::EFormat format = TYsonWriter::EFormat::Binary)
{
    TYsonWriter writer(&output, format);
    TTreeVisitor visitor(&writer);
    visitor.Visit(node);
    return output;
}

inline TYson SerializeToYson(
    const INode* node,
    TYsonWriter::EFormat format = TYsonWriter::EFormat::Binary)
{
    TStringStream output;
    SerializeToYson(node, output, format);
    return output.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
