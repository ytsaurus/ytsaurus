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

inline INode::TPtr DeserializeFromYson(TInputStream* istream,
    INodeFactory* factory = GetEphemeralNodeFactory())
{
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    TYsonReader reader(~builder);
    reader.Read(istream);
    return builder->EndTree();
}

inline INode::TPtr DeserializeFromYson(const Stroka& string,
    INodeFactory* factory = GetEphemeralNodeFactory())
{
    TStringInput stream(string);
    return DeserializeFromYson(&stream, factory);
}

inline TOutputStream& SerializeToYson(
    const INode::TPtr& node,
    const TYsonWriter::EFormat& format,
    TOutputStream& ostream)
{
    TYsonWriter writer(&ostream, format);
    TTreeVisitor visitor(&writer);
    visitor.Visit(node);
    return ostream;
}

inline Stroka SerializeToYson(
    const INode::TPtr& node,
    const TYsonWriter::EFormat& format)
{
    TStringStream stream;
    SerializeToYson(node, format, stream);
    return stream.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
