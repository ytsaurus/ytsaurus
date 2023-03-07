#include "shard.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/core/misc/serialize.h>

#include <yt/server/master/security_server/account.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool TCypressShardAccountStatistics::IsZero() const
{
    return NodeCount == 0;
}

void TCypressShardAccountStatistics::Persist(NCellMaster::TPersistenceContext& context)
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
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, AccountStatistics_);
    Save(context, Root_);
    Save(context, Name_);
}

void TCypressShard::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, AccountStatistics_);
    Load(context, Root_);
    // COMPAT(babenko)
    if (context.GetVersion() >= EMasterReign::CypressShardName) {
        Load(context, Name_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

