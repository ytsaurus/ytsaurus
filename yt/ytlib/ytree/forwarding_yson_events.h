#pragma once

#include "common.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public IYsonConsumer
{
protected:
    TForwardingYsonConsumer();

    void ForwardNode(IYsonConsumer* consumer, IAction::TPtr onForwardingFinished);
    void ForwardAttributes(IYsonConsumer* consumer, IAction::TPtr onForwardingFinished);

    virtual void OnMyStringScalar(const Stroka& value, bool hasAttributes) = 0;
    virtual void OnMyInt64Scalar(i64 value, bool hasAttributes) = 0;
    virtual void OnMyDoubleScalar(double value, bool hasAttributes) = 0;
    virtual void OnMyEntity(bool hasAttributes) = 0;

    virtual void OnMyBeginList() = 0;
    virtual void OnMyListItem() = 0;
    virtual void OnMyEndList(bool hasAttributes) = 0;

    virtual void OnMyBeginMap() = 0;
    virtual void OnMyMapItem(const Stroka& name) = 0;
    virtual void OnMyEndMap(bool hasAttributes) = 0;

    virtual void OnMyBeginAttributes() = 0;
    virtual void OnMyAttributesItem(const Stroka& name) = 0;
    virtual void OnMyEndAttributes() = 0;

private:
    IYsonConsumer* ForwardingConsumer;
    int ForwardingDepth;
    IAction::TPtr OnForwardingFinished;

    virtual void OnStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnDoubleScalar(double value, bool hasAttributes);
    virtual void OnEntity(bool hasAttributes);

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList(bool hasAttributes);

    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap(bool hasAttributes);

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();

    void DoForward(IYsonConsumer* consumer, IAction::TPtr onForwardingFinished, int depth);
    void UpdateDepth(int depthDelta);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
