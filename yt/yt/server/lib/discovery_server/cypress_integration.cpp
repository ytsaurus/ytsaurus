#include "cypress_integration.h"
#include "group_tree.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NDiscoveryServer {

using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NDiscoveryClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryYPathService
    : public TYPathServiceBase
{
public:
    explicit TDiscoveryYPathService(
        TGroupTreePtr groupTree)
        : GroupTree_(std::move(groupTree))
    { }

    TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

private:
    const TGroupTreePtr GroupTree_;

    bool DoInvoke(const IServiceContextPtr& context) override
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

        auto attributeKeys = request->has_attributes()
            ? std::make_optional(FromProto<std::vector<TString>>(request->attributes().keys()))
            : std::nullopt;

        response->set_value(GroupTree_->List(path, attributeKeys).ToString());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NYT::NYTree::NProto, Get)
    {
        const auto& path = GetRequestTargetYPath(context->RequestHeader());

        auto attributeKeys = request->has_attributes()
            ? std::make_optional(FromProto<std::vector<TString>>(request->attributes().keys()))
            : std::nullopt;

        response->set_value(GroupTree_->Get(path, attributeKeys).ToString());

        context->Reply();
    }
};

IYPathServicePtr CreateDiscoveryYPathService(TGroupTreePtr groupTree)
{
    return New<TDiscoveryYPathService>(std::move(groupTree));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
