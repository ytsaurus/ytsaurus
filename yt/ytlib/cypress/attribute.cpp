#include "stdafx.h"
#include "attribute.h"
#include "cypress_manager.h"
#include "node_proxy.h"

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

} // namespace NCypress
} // namespace NYT

