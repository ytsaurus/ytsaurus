#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/yson/consumer.h>

#include <util/generic/stack.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeBuilder
    : public TYsonConsumerBase
{
public:
    TNodeBuilder(TNode* node);

    virtual void OnStringScalar(const TStringBuf&) override;
    virtual void OnInt64Scalar(i64) override;
    virtual void OnUint64Scalar(ui64) override;
    virtual void OnDoubleScalar(double) override;
    virtual void OnBooleanScalar(bool) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf&) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

private:
    ystack<TNode*> Stack_;

private:
    inline void AddNode(TNode node, bool pop);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
