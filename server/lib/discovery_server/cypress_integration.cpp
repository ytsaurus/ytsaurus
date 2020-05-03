#include "cypress_integration.h"
#include "group_tree.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_detail.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/retrying_channel.h>

namespace NYT::NDiscoveryServer {

using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NDiscoveryClient;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryYPathService
    : public TYPathServiceBase
{
public:
    explicit TDiscoveryYPathService(
        TGroupTreePtr groupTree)
        : GroupTree_(std::move(groupTree))
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const std::optional<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    virtual bool ShouldHideAttributes() override
    {
        YT_ABORT();
    }

private:
    const TGroupTreePtr GroupTree_;

    bool DoInvoke(const IServiceContextPtr& context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        DISPATCH_YPATH_SERVICE_METHOD(List);
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        return false;
    }

    DECLARE_YPATH_SERVICE_METHOD(NYT::NYTree::NProto, Exists)
    {
        const auto& path = GetRequestTargetYPath(context->RequestHeader());
        response->set_value(GroupTree_->Exists(path));

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NYT::NYTree::NProto, List)
    {
        const auto& path = GetRequestTargetYPath(context->RequestHeader());
        response->set_value(GroupTree_->List(path).GetData());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NYT::NYTree::NProto, Get)
    {
        const auto& path = GetRequestTargetYPath(context->RequestHeader());
        response->set_value(GroupTree_->Get(path).GetData());

        context->Reply();
    }
};

IYPathServicePtr CreateDiscoveryYPathService(TGroupTreePtr groupTree)
{
    return New<TDiscoveryYPathService>(std::move(groupTree));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
