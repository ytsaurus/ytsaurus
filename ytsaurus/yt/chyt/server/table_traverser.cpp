#include "table_traverser.h"

#include "clickhouse_service_proxy.h"

#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

std::vector<NYTree::INodePtr> GetNodes(
    const NNative::IClientPtr& client,
    const TMasterReadOptions& masterReadOptions,
    const std::vector<TString>& paths)
{
    std::vector<TString> attributeKeys = {"type", "path", "opaque"};

    auto connection = client->GetNativeConnection();

    TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(masterReadOptions.ReadFrom));
    auto batchRequest = proxy.ExecuteBatch();
    SetBalancingHeader(batchRequest, connection, masterReadOptions);

    for (const auto& path : paths) {
        auto request = TYPathProxy::Get(path);
        ToProto(request->mutable_attributes()->mutable_keys(), attributeKeys);
        SetCachingHeader(request, connection, masterReadOptions);

        batchRequest->AddRequest(request, path);
    }

    auto batchResponse = WaitFor(batchRequest->Invoke())
        .ValueOrThrow();

    std::vector<NYTree::INodePtr> nodes;
    for (size_t index = 0; index < paths.size(); ++index){
        auto response = batchResponse->GetResponse<TYPathProxy::TRspGet>(index)
            .ValueOrThrow();
        nodes.push_back(ConvertToNode(TYsonString(response->value())));
    }
    return nodes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TTableTraverser::TTableTraverser(
    NNative::IClientPtr client,
    const TMasterReadOptions& masterReadOptions,
    std::vector<TString> roots,
    const FilterByNameFunction& filterByTableName)
    : Client_(std::move(client))
    , MasterReadOptions_(masterReadOptions)
    , Roots_(std::move(roots))
    , FilterByTableName_(filterByTableName)
{
    TraverseTablesFromRoots();
}

const std::vector<std::string>& TTableTraverser::GetTables() const
{
    return Tables_;
}

void TTableTraverser::TraverseTablesFromRoots()
{
    auto nodes = NDetail::GetNodes(Client_, MasterReadOptions_, Roots_);

    YT_VERIFY(nodes.size() == Roots_.size());
    for (size_t index = 0; index < Roots_.size(); ++index) {
        TraverseTablesFromNode(nodes[index], Roots_[index]);
    }
}

void TTableTraverser::TraverseTablesFromNode(NYTree::INodePtr node, const TString& dirName)
{
    if (node->GetType() == ENodeType::Entity) {
        bool opaque = node->Attributes().Get<bool>("opaque", false);
        if (opaque) {
            // If node is marked as opaque, entity value is expected.
            // Ignore such nodes.
            return;
        } else {
            THROW_ERROR_EXCEPTION("Object count limit exceeded while getting directory %Qv; "
                "consider specifying smaller dirs or exclude unused subdirs via setting opaque attribute",
                dirName);
        }
    }

    for (const auto& [path, child] : node->AsMap()->GetChildren()) {
        auto type = child->Attributes().Get<EObjectType>("type");
        if (type == EObjectType::Table) {
            auto path = child->Attributes().Get<TString>("path");
            if (!FilterByTableName_ || FilterByTableName_(path)) {
                Tables_.emplace_back(std::move(path));
            }
        } else if (type == EObjectType::MapNode) {
            TraverseTablesFromNode(child, dirName);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
