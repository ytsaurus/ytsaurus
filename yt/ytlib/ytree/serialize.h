#pragma once

#include "ephemeral.h"
#include "yson_writer.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CloneNode(
    const INode* node,
    INodeFactory* factory = GetEphemeralNodeFactory());

TYsonProducer::TPtr ProducerFromYson(TInputStream* input);

TYsonProducer::TPtr ProducerFromYson(const TYson& data);

TYsonProducer::TPtr ProducerFromNode(const INode* node);

INode::TPtr DeserializeFromYson(
    TInputStream* input,
    INodeFactory* factory = GetEphemeralNodeFactory());

INode::TPtr DeserializeFromYson(
    const TYson& yson,
    INodeFactory* factory = GetEphemeralNodeFactory());

TOutputStream& SerializeToYson(
    const INode* node,
    TOutputStream& output,
    EFormat format = EFormat::Binary);

TYson SerializeToYson(
    const INode* node,
    EFormat format = EFormat::Binary);

TYson SerializeToYson(
    TYsonProducer* producer,
    EFormat format = EFormat::Binary);

TYson SerializeToYson(
    const TConfigurable* config,
    EFormat format = EFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
