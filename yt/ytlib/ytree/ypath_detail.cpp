#include "../misc/stdafx.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase()
    : FwdConsumer(NULL)
    , FwdDepth(0)
{ }

void TNodeSetterBase::InvalidType()
{
    throw TYTreeException() << "Cannot change node type";
}

void TNodeSetterBase::SetFwdConsumer(IYsonConsumer* consumer)
{
    YASSERT(FwdConsumer == NULL);
    FwdConsumer = consumer;
    FwdDepth = 0;
}

void TNodeSetterBase::OnFwdConsumerFinished()
{ }

void TNodeSetterBase::UpdateFwdDepth(int depthDelta)
{
    FwdDepth += depthDelta;
    YASSERT(FwdDepth >= 0);
    if (FwdDepth == 0) {
        FwdConsumer = NULL;
        OnFwdConsumerFinished();
    }
}

void TNodeSetterBase::OnStringScalar(const Stroka& value)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnStringScalar(value);
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnInt64Scalar(i64 value)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnInt64Scalar(value);
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnDoubleScalar(double value)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnDoubleScalar(value);
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnEntityScalar()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEntityScalar();
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnBeginList()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnBeginList();
        UpdateFwdDepth(+1);
    }
}

void TNodeSetterBase::OnListItem(int index)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnListItem(index);
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnEndList()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEndList();
        UpdateFwdDepth(-1);
    }
}

void TNodeSetterBase::OnBeginMap()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnBeginMap();
        UpdateFwdDepth(+1);
    }
}

void TNodeSetterBase::OnMapItem(const Stroka& name)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnMapItem(name);
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnEndMap()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEndMap();
        UpdateFwdDepth(-1);
    }
}

void TNodeSetterBase::OnBeginAttributes()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnBeginAttributes();
        UpdateFwdDepth(+1);
    }
}

void TNodeSetterBase::OnAttributesItem(const Stroka& name)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnAttributesItem(name);
        UpdateFwdDepth(0);
    }
}

void TNodeSetterBase::OnEndAttributes()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEndAttributes();
        UpdateFwdDepth(-1);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
