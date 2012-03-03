#pragma once

#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public virtual IYsonConsumer
{
protected:
    TForwardingYsonConsumer();

    void ForwardNode(IYsonConsumer* consumer, IAction* onForwardingFinished);
    void ForwardAttributes(IYsonConsumer* consumer, IAction* onForwardingFinished);

    virtual void OnMyStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnMyInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnMyDoubleScalar(double value, bool hasAttributes);
    virtual void OnMyEntity(bool hasAttributes);

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList(bool hasAttributes);

    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const Stroka& name);
    virtual void OnMyEndMap(bool hasAttributes);

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const Stroka& name);
    virtual void OnMyEndAttributes();

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

    void DoForward(IYsonConsumer* consumer, IAction* onForwardingFinished, int depth);
    void UpdateDepth(int depthDelta);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
