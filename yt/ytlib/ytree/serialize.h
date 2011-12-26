#pragma once

#include "ephemeral.h"
#include "yson_writer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CloneNode(
    const INode* node,
    INodeFactory* factory = GetEphemeralNodeFactory());

TYsonProducer::TPtr ProducerFromYson(TInputStream* input);

INode::TPtr DeserializeFromYson(
    TInputStream* input,
    INodeFactory* factory = GetEphemeralNodeFactory());

INode::TPtr DeserializeFromYson(
    const TYson& yson,
    INodeFactory* factory = GetEphemeralNodeFactory());

TOutputStream& SerializeToYson(
    const INode* node,
    TOutputStream& output,
    TYsonWriter::EFormat format = TYsonWriter::EFormat::Binary);

TYson SerializeToYson(
    const INode* node,
    TYsonWriter::EFormat format = TYsonWriter::EFormat::Binary);

TYson SerializeToYson(
    TYsonProducer* producer,
    TYsonWriter::EFormat format = TYsonWriter::EFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
