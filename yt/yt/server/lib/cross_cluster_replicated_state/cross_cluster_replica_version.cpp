#include "cross_cluster_replica_version.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NCrossClusterReplicatedState {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TReplicaVersion ExtractVersion(const INodePtr& node)
{
    if (!node || node->GetType() == ENodeType::Entity) {
        return NullReplicaVersion;
    }

    auto map = node->AsMap();
    auto version = map
        ->GetChildOrThrow("version")
        ->AsUint64()
        ->GetValue();
    auto tag = map
        ->GetChildOrThrow("version_tag")
        ->AsString()
        ->GetValue();

    return {version, std::move(tag)};
}

void PutVersion(const IMapNodePtr& node, const TReplicaVersion& version)
{
    node->RemoveChild("version");
    node->AddChild("version", ConvertToNode(version.first));
    node->RemoveChild("version_tag");
    node->AddChild("version_tag", ConvertToNode(version.second));
}

std::string MakeVersionAttributeValue(const TReplicaVersion& version)
{
    return std::format("{:016x};{}", version.first, version.second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
