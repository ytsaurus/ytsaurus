#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase()
    : FwdConsumer(NULL)
{ }

TNodeSetterBase::~TNodeSetterBase()
{ }

void TNodeSetterBase::InvalidType()
{
    throw TYPathException() << "Cannot change node type";
}

void TNodeSetterBase::SetFwdConsumer(IYsonConsumer* consumer)
{
    FwdConsumer = consumer;
}

void TNodeSetterBase::ResetFwdConsumer()
{
    FwdConsumer = NULL;
}

void TNodeSetterBase::OnStringScalar(const Stroka& value)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnStringScalar(value);
    }
}

void TNodeSetterBase::OnInt64Scalar(i64 value)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnInt64Scalar(value);
    }
}

void TNodeSetterBase::OnDoubleScalar(double value)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnDoubleScalar(value);
    }
}

void TNodeSetterBase::OnEntityScalar()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEntityScalar();
    }
}

void TNodeSetterBase::OnBeginList()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnBeginList();
    }
}

void TNodeSetterBase::OnListItem(int index)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnListItem(index);
    }
}

void TNodeSetterBase::OnEndList()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEndList();
    }
}

void TNodeSetterBase::OnBeginMap()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnBeginMap();
    }
}

void TNodeSetterBase::OnMapItem(const Stroka& name)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnMapItem(name);
    }
}

void TNodeSetterBase::OnEndMap()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEndMap();
    }
}

void TNodeSetterBase::OnBeginAttributes()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnBeginAttributes();
    }
}

void TNodeSetterBase::OnAttributesItem(const Stroka& name)
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnAttributesItem(name);
    }
}

void TNodeSetterBase::OnEndAttributes()
{
    if (FwdConsumer == NULL) {
        InvalidType();
    } else {
        FwdConsumer->OnEndAttributes();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
