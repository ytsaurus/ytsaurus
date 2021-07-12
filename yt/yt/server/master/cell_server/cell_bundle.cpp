#include "config.h"
#include "private.h"
#include "cell_bundle.h"
#include "cell_base.h"
#include "yt/yt/server/master/tablet_server/private.h"

#include <yt/yt/server/master/tablet_server/tablet_action.h>
#include <yt/yt/server/master/tablet_server/tablet_cell.h>
#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>
#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/profiling/profile_manager.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TCellBundle::TCellBundle(TCellBundleId id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
    , Options_(New<TTabletCellOptions>())
    , CellBalancerConfig_(New<TCellBalancerConfig>())
    , Health_(ECellHealth::Failed)
    , DynamicOptions_(New<TDynamicTabletCellOptions>())
{ }

TString TCellBundle::GetLowercaseObjectName() const
{
    return Format("cell bundle %Qv", Name_);
}

TString TCellBundle::GetCapitalizedObjectName() const
{
    return Format("Cell bundle %Qv", Name_);
}

void TCellBundle::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Acd_);
    Save(context, *Options_);
    Save(context, *DynamicOptions_);
    Save(context, DynamicConfigVersion_);
    Save(context, *CellBalancerConfig_);
    Save(context, Health_);
}

void TCellBundle::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Acd_);
    Load(context, *Options_);
    Load(context, *DynamicOptions_);
    Load(context, DynamicConfigVersion_);
    if (context.GetVersion() < EMasterReign::Areas) {
        Load(context, NodeTagFilter_);
    }
    Load(context, *CellBalancerConfig_);
    Load(context, Health_);

    InitializeProfilingCounters();
}

void TCellBundle::SetName(TString name)
{
    Name_ = name;
    InitializeProfilingCounters();
}

TString TCellBundle::GetName() const
{
    return Name_;
}

TDynamicTabletCellOptionsPtr TCellBundle::GetDynamicOptions() const
{
    return DynamicOptions_;
}

void TCellBundle::SetDynamicOptions(TDynamicTabletCellOptionsPtr dynamicOptions)
{
    DynamicOptions_ = std::move(dynamicOptions);
    ++DynamicConfigVersion_;
}

void TCellBundle::InitializeProfilingCounters()
{
    auto profiler = TabletServerProfiler
        .WithTag("tablet_cell_bundle", Name_);

    ProfilingCounters_.Profiler = profiler;
    ProfilingCounters_.TabletCellCount = profiler.WithSparse().Gauge("/tablet_cell_count");
    ProfilingCounters_.ReplicaSwitch = profiler.Counter("/switch_tablet_replica_mode");
    ProfilingCounters_.InMemoryMoves = profiler.Counter("/tablet_balancer/in_memory_moves");
    ProfilingCounters_.ExtMemoryMoves = profiler.Counter("/tablet_balancer/ext_memory_moves");
    ProfilingCounters_.TabletMerges = profiler.Counter("/tablet_balancer/tablet_merges");
    ProfilingCounters_.TabletCellMoves = profiler.Counter("/tablet_tracker/tablet_cell_moves");

    ProfilingCounters_.PeerAssignment = profiler.Counter("/tablet_tracker/peer_assignment");
}

TCounter& TCellBundleProfilingCounters::GetLeaderReassignment(const TString& reason)
{
    auto it = LeaderReassignment.find(reason);
    if (it == LeaderReassignment.end()) {
        it = LeaderReassignment.emplace(
            reason,
            Profiler.WithTag("reason", reason).Counter("/tablet_tracker/leader_reassignment")).first;
    }
    return it->second;
}

TCounter& TCellBundleProfilingCounters::GetPeerRevocation(const TString& reason)
{
    auto it = PeerRevocation.find(reason);
    if (it == PeerRevocation.end()) {
        it = PeerRevocation.emplace(
            reason,
            Profiler.WithTag("reason", reason).Counter("/tablet_tracker/peer_revocation")).first;
    }
    return it->second;
}

TArea* TCellBundle::GetAreaOrThrow(const TString& name)
{
    auto it = Areas_.find(name);
    if (!it) {
        THROW_ERROR_EXCEPTION("Cell bundle %Qv has no area named %Qv",
            GetName(),
            name);
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
