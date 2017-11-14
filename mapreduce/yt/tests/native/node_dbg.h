#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/common/node_visitor.h>

#include <library/dbg_output/engine.h>
#include <library/yson/writer.h>

template <>
struct TDumper<NYT::TNode>
{
    template <class S>
    static inline void Dump(S& s, const NYT::TNode& node)
    {
        using namespace NYT;
        if (node.GetType() == TNode::Undefined) {
            s.Stream() << "UNDEFINED";
        } else {
            TYsonWriter writer(&s.Stream(), EYsonFormat::YF_TEXT);
            TNodeVisitor(&writer).Visit(node);
        }
    }
};
