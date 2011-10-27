#pragma once

#include "common.h"
#include "attribute.h"
#include "cypress_manager.h"
#include "node_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeAttributeProvider
    : public TAttributeProviderBase
{
public:
    static IAttributeProvider* Get();

    TCypressNodeAttributeProvider();

protected:
    template <class TProxy>
    static TIntrusivePtr<TProxy> GetProxy(const TGetRequest& request)
    {
        TIntrusivePtr<TProxy> typedProxy(dynamic_cast<TProxy*>(request.Proxy));
        YASSERT(~typedProxy != NULL);
        return typedProxy;
    }

    template <class TImpl>
    static const TImpl& GetImpl(const TGetRequest& request)
    {
        const auto& impl = request.CypressManager->GetTransactionNode(
            request.Proxy->GetNodeId(),
            request.Proxy->GetTransactionId());
        return dynamic_cast<const TImpl&>(impl);
    }

private:
    typedef TCypressNodeAttributeProvider TThis;

    static void GetNodeId(const TGetRequest& request);
    static void GetType(const TGetRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeAttributeProvider
    : public TCypressNodeAttributeProvider
{
public:
    static IAttributeProvider* Get();

    TCompositeNodeAttributeProvider();

private:
    typedef TCompositeNodeAttributeProvider TThis;

    static void GetSize(const TGetRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
