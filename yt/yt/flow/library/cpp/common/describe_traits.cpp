#include "describe_traits.h"

#include "registry.h"
#include "yt_path_option.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NFlow {

using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

void MakeLinksInplace(const INodePtr& node, const std::optional<std::string>& defaultCluster)
{
    if (node->GetType() == ENodeType::Map) {
        auto mapNode = node->AsMap();
        for (const auto& [key, value] : mapNode->GetChildren()) {
            if (value->GetType() == ENodeType::String) {
                TRichYPath path;

                // Check if string looks like a path.
                try {
                    path = TRichYPath::Parse(value->AsString()->GetValue());
                } catch (...) {
                    continue;
                }

                if (!path.GetPath().StartsWith("//")) {
                    continue;
                }

                // Use the path's own cluster if present, otherwise the pipeline's cluster.
                auto cluster = path.GetCluster().has_value() ? path.GetCluster() : defaultCluster;
                if (!cluster.has_value()) {
                    continue;
                }
                auto href = Format("/%v/navigation?path=%v", *cluster, path.GetPath());

                mapNode->RemoveChild(key);
                // clang-format off
                mapNode->AddChild(
                    key,
                    BuildYsonNodeFluently()
                        .BeginAttributes()
                            .Item("_type_tag").Value("url")
                        .EndAttributes()
                        .BeginMap()
                            .Item("href").Value(href)
                            .Item("text").Value(value->AsString()->GetValue())
                        .EndMap());
                // clang-format on
            } else {
                MakeLinksInplace(value, defaultCluster);
            }
        }
    } else if (node->GetType() == ENodeType::List) {
        for (const auto& child : node->AsList()->GetChildren()) {
            MakeLinksInplace(child, defaultCluster);
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDescribeTraitsBase::TDescribeTraitsBase(TDescribeTraitsContext context)
    : Context_(std::move(context))
{ }

void TDescribeTraitsBase::MakeLinks(const IMapNodePtr& parameters) const
{
    MakeLinksInplace(parameters, Context_.PipelinePath.GetCluster());
}

std::vector<TDataChannel> TDescribeTraitsBase::GetDataChannels(
    const TYsonStructPtr& parameters,
    const TEntityControllerStateMap& /*states*/) const
{
    std::vector<TDataChannel> channels;
    const TYTPathClaim* previousClaim = nullptr;
    for (const auto& claim : TRegistry::Get()->CollectYTPathClaims(parameters, Context_.PipelinePath.GetCluster())) {
        // A path on several clusters yields consecutive claims that differ only in the cluster: one channel for them.
        bool samePath = previousClaim &&
            previousClaim->ParameterKey == claim.ParameterKey &&
            previousClaim->Path == claim.Path &&
            previousClaim->Ownership == claim.Ownership;
        if (!samePath) {
            channels.emplace_back().Access = claim.Ownership == EYTPathOwnership::ReadOnly
                ? EDataChannelAccess::Read
                : EDataChannelAccess::Write;
        }

        channels.back().Aliases.push_back(Format("yt:%v:%v", claim.Cluster, claim.Path));
        previousClaim = &claim;
    }

    return channels;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
