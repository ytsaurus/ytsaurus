#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral.h"
#include "ypath_client.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr IYPathService::FromProducer(TYsonProducer producer)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    producer.Run(~builder);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TViaYPathService
    : public TYPathServiceBase
{
public:
    TViaYPathService(IYPathServicePtr underlyingService, IInvokerPtr invoker)
        : UnderlyingService(underlyingService)
        , Invoker(invoker)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        return TResolveResult::Here(path);
    }

private:
    IYPathServicePtr UnderlyingService;
    IInvokerPtr Invoker;

    virtual void DoInvoke(NRpc::IServiceContextPtr context)
    {
        Invoker->Invoke(BIND(
            &TViaYPathService::ExecuteRequest,
            MakeStrong(this),
            context));
    }

    void ExecuteRequest(NRpc::IServiceContextPtr context)
    {
        try {
            ExecuteVerb(UnderlyingService, ~context);
        } catch (const std::exception& ex) {
            context->Reply(TError(ex.what()));
        }
    }
};

} // namespace

IYPathServicePtr IYPathService::Via(IInvokerPtr invoker)
{
    return New<TViaYPathService>(this, invoker);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TFromProducerPathService
    : public TYPathServiceBase
{
public:
    TFromProducerPathService(TYPathServiceProducer producer)
        : Producer(producer)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        return TResolveResult::Here(path);
    }

private:
    TYPathServiceProducer Producer;

    virtual void DoInvoke(NRpc::IServiceContextPtr context)
    {
        auto service = Producer.Run();
        ExecuteVerb(~service, context);
    }
};

} // namespace

IYPathServicePtr IYPathService::FromProducer(TYPathServiceProducer producer)
{
    return New<TFromProducerPathService>(producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
