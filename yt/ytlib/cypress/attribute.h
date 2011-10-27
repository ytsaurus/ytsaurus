#pragma once

#include "common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class  TCypressManager;
struct ICypressNodeProxy;

struct IAttributeProvider
{
    virtual ~IAttributeProvider()
    { }

    virtual void GetAttributeNames(
        TIntrusivePtr<TCypressManager> cypressManager,
        TIntrusivePtr<ICypressNodeProxy> proxy,
        yvector<Stroka>& names) = 0;

    virtual bool GetAttribute(
        TIntrusivePtr<TCypressManager> cypressManager,
        TIntrusivePtr<ICypressNodeProxy> proxy,
        const Stroka& name,
        IYsonConsumer* consumer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TAttributeProviderBase
    : public IAttributeProvider
{
public:
    virtual void GetAttributeNames(
        TIntrusivePtr<TCypressManager> cypressManager,
        TIntrusivePtr<ICypressNodeProxy> proxy,
        yvector<Stroka>& names);

    virtual bool GetAttribute(
        TIntrusivePtr<TCypressManager> cypressManager,
        TIntrusivePtr<ICypressNodeProxy> proxy,
        const Stroka& name,
        IYsonConsumer* consumer);

protected:
    struct TGetRequest
    {
        TIntrusivePtr<TCypressManager> CypressManager;
        TIntrusivePtr<ICypressNodeProxy> Proxy;
        IYsonConsumer* Consumer;
    };

    typedef IParamAction<const TGetRequest&> TGetter;

    yhash_map<Stroka, TGetter::TPtr> Getters;

    void RegisterGetter(const Stroka& name, TGetter::TPtr getter);
};

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
