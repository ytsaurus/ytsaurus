#pragma once

#include "common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy;
struct TCypressManager;

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

} // namespace NCypress
} // namespace NYT
