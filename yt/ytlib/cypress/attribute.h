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

private:
    typedef TCypressNodeAttributeProvider TThis;

    static void GetId(const TGetRequest& request);
    static void GetType(const TGetRequest& request);
    static Stroka FormatType(ENodeType type);
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
