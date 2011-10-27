#include "stdafx.h"
#include "attribute_detail.h"

#include "../ytree/fluent.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

IAttributeProvider* TCypressNodeAttributeProvider::Get()
{
    return Singleton<TCypressNodeAttributeProvider>();
}

TCypressNodeAttributeProvider::TCypressNodeAttributeProvider()
{
    RegisterGetter("node_id", FromMethod(&TThis::GetNodeId));
    RegisterGetter("type", FromMethod(&TThis::GetType));
}

void TCypressNodeAttributeProvider::GetNodeId(const TGetRequest& request)
{
    BuildYsonFluently(request.Consumer)
        .Scalar(request.Proxy->GetNodeId().ToString());
}

void TCypressNodeAttributeProvider::GetType(const TGetRequest& request)
{
    BuildYsonFluently(request.Consumer)
        .Scalar(request.Proxy->GetTypeName());
}

////////////////////////////////////////////////////////////////////////////////

IAttributeProvider* TCompositeNodeAttributeProvider::Get()
{
    return Singleton<TCompositeNodeAttributeProvider>();
}

TCompositeNodeAttributeProvider::TCompositeNodeAttributeProvider()
{
    RegisterGetter("size", FromMethod(&TThis::GetSize));
}

void TCompositeNodeAttributeProvider::GetSize(const TGetRequest& request)
{
    auto* typedProxy = dynamic_cast<ICompositeNode*>(~request.Proxy);
    YASSERT(typedProxy != NULL);
    BuildYsonFluently(request.Consumer)
        .Scalar(typedProxy->GetChildCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

