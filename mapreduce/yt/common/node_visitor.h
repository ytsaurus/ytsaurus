#pragma once

#include <mapreduce/yt/interface/node.h>
#include <library/yson/consumer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeVisitor
{
public:
    TNodeVisitor(IYsonConsumer* consumer);

    void Visit(const TNode& node);
    void VisitMap(const TNode::TMap& nodeMap);
    void VisitList(const TNode::TList& nodeMap);

private:
    IYsonConsumer* Consumer_;

private:
    void VisitAny(const TNode& node);

    void VisitString(const TNode& node);
    void VisitInt64(const TNode& node);
    void VisitUint64(const TNode& node);
    void VisitDouble(const TNode& node);
    void VisitBool(const TNode& node);
    void VisitEntity();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
