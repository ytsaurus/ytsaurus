#include "table_traverser.h"

#include "clickhouse_service_proxy.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NClickHouseServer {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

std::vector<NYTree::INodePtr> GetNodes(NObjectClient::TObjectServiceProxy proxy, const std::vector<TString>& paths)
{
    std::vector<TString> attributeKeys = {"type", "path", "opaque"};

    auto batchRequest = proxy.ExecuteBatch();
    for (const auto& path : paths) {
        auto request = TYPathProxy::Get(path);
        ToProto(request->mutable_attributes()->mutable_keys(), attributeKeys);
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

TTableTraverser::TTableTraverser(IClientPtr client, std::vector<TString> roots, const FilterByNameFunction& filterByTableName)
    : Client_(std::move(client))
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
    auto channel = Client_->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Follower, NObjectClient::PrimaryMasterCellTag);
    TClickHouseServiceProxy proxy(channel);

    auto nodes = NDetail::GetNodes(NObjectClient::TObjectServiceProxy(channel), Roots_);

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
