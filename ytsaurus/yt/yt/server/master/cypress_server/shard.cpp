#include "shard.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/cypress_server/portal_exit_node.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool TCypressShardAccountStatistics::IsZero() const
{
    return NodeCount == 0;
}

void TCypressShardAccountStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, NodeCount);
}

void Serialize(const TCypressShardAccountStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_count").Value(statistics.NodeCount)
        .EndMap();
}

TCypressShardAccountStatistics& operator +=(
    TCypressShardAccountStatistics& lhs,
    const TCypressShardAccountStatistics& rhs)
{
    lhs.NodeCount += rhs.NodeCount;
    return lhs;
}

TCypressShardAccountStatistics operator +(
    const TCypressShardAccountStatistics& lhs,
    const TCypressShardAccountStatistics& rhs)
{
    TCypressShardAccountStatistics result;
    result += lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TCypressShardAccountStatistics TCypressShard::ComputeTotalAccountStatistics() const
{
    TCypressShardAccountStatistics result;
    for (const auto& [account, statistics] : AccountStatistics_) {
        result += statistics;
    }
    return result;
}

TString TCypressShard::GetLowercaseObjectName() const
{
    return Format("Cypress shard %v", GetId());
}

TString TCypressShard::GetCapitalizedObjectName() const
{
    return Format("Cypress shard %v", GetId());
}

void TCypressShard::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, AccountStatistics_);
    Save(context, Root_);
    Save(context, Name_);
}

void TCypressShard::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, AccountStatistics_);
    Load(context, Root_);
    Load(context, Name_);
}

NYPath::TYPath TCypressShard::MaybeRewritePath(const NYPath::TYPath& path)
{
    auto rootNode = GetRoot();
    if (rootNode->GetType() == NCypressClient::EObjectType::PortalExit) {
        auto exitNode = rootNode->As<TPortalExitNode>();
        auto pathPrefix = exitNode->GetPath();
        if (path.length() != pathPrefix.length() && path.StartsWith(pathPrefix)) {
            return NObjectClient::FromObjectId(exitNode->GetId()) + path.substr(pathPrefix.length());
        }
    }
    return path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

