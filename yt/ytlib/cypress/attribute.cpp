#include "stdafx.h"
#include "attribute.h"
#include "cypress_manager.h"
#include "node_proxy.h"

#include "../ytree/fluent.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

void TAttributeProviderBase::GetAttributeNames(
    TCypressManager::TPtr cypressManager,
    ICypressNodeProxy::TPtr proxy,
    yvector<Stroka>& names)
{
    UNUSED(cypressManager);
    UNUSED(proxy);
    FOREACH (const auto& pair, Getters) {
        names.push_back(pair.First());
    }
}

bool TAttributeProviderBase::GetAttribute(
    TCypressManager::TPtr cypressManager,
    ICypressNodeProxy::TPtr proxy,
    const Stroka& name,
    IYsonConsumer* consumer)
{
    auto it = Getters.find(name);
    if (it == Getters.end())
        return false;

    TGetRequest request;
    request.CypressManager = cypressManager;
    request.Proxy = proxy;
    request.Consumer = consumer;

    it->Second()->Do(request);
    return true;
}

void TAttributeProviderBase::RegisterGetter(const Stroka& name, TGetter::TPtr getter)
{
    YVERIFY(Getters.insert(MakePair(name, getter)).Second());
}

////////////////////////////////////////////////////////////////////////////////

IAttributeProvider* TCypressNodeAttributeProvider::Get()
{
    return Singleton<TCypressNodeAttributeProvider>();
}

TCypressNodeAttributeProvider::TCypressNodeAttributeProvider()
{
    RegisterGetter("id", FromMethod(&TThis::GetId));
    RegisterGetter("type", FromMethod(&TThis::GetType));
}

void TCypressNodeAttributeProvider::GetId(const TGetRequest& request)
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

