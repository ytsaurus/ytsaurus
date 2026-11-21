#include "config.h"
#include "private.h"
#include "cell_bundle.h"
#include "cell_base.h"

#include <yt/yt/server/master/tablet_server/private.h>
#include <yt/yt/server/master/tablet_server/tablet_action.h>
#include <yt/yt/server/master/tablet_server/tablet_cell.h>
#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>
#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cellar_agent/helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NChunkClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TCellBundle::TCellBundle(TCellBundleId id)
    : TObject(id)
    , Acd_(this)
    , Options_(New<TTabletCellOptions>())
    , CellBalancerConfig_(New<TCellBalancerBootstrapConfig>())
    , Health_(ECellHealth::Failed)
    , DynamicOptions_(New<TDynamicTabletCellOptions>())
{ }

std::string TCellBundle::GetLowercaseObjectName() const
{
    return Format("cell bundle %Qv", Name_);
}

std::string TCellBundle::GetCapitalizedObjectName() const
{
    return Format("Cell bundle %Qv", Name_);
}

void TCellBundle::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, Acd_);
    PHOENIX_REGISTER_FIELD(3, Options_,
        .template Serializer<TDerefSerializer<>>());
    PHOENIX_REGISTER_FIELD(4, DynamicOptions_,
        .template Serializer<TDerefSerializer<>>());
    PHOENIX_REGISTER_FIELD(5, DynamicConfigVersion_);
    PHOENIX_REGISTER_FIELD(6, CellBalancerConfig_,
        .template Serializer<TDerefSerializer<>>());
    PHOENIX_REGISTER_FIELD(7, Health_);
    PHOENIX_REGISTER_FIELD(8, ConfigVersion_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->InitializeProfilingCounters();
    });
}

PHOENIX_DEFINE_TYPE(TCellBundle);

void TCellBundle::SetName(const std::string& name)
{
    Name_ = name;
    InitializeProfilingCounters();
}

const std::string& TCellBundle::GetName() const
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

ECellarType TCellBundle::GetCellarType() const
{
    return GetCellarTypeFromCellBundleId(GetId());
}

void TCellBundle::InitializeProfilingCounters()
{
    auto profiler = TabletServerProfiler()
        .WithTag("tablet_cell_bundle", Name_);

    ProfilingCounters_.Profiler = profiler;
    for (auto health : TEnumTraits<ECellHealth>::GetDomainValues()) {
        ProfilingCounters_.TabletCellCount[health] = profiler.WithSparse()
            .WithTag("health", FormatEnum(health))
            .Gauge("/tablet_cell_count");
    }

    ProfilingCounters_.ReplicaModeSwitch = profiler.Counter("/replica_mode_switch");
    ProfilingCounters_.InMemoryMoves = profiler.Counter("/tablet_balancer/in_memory_moves");
    ProfilingCounters_.ExtMemoryMoves = profiler.Counter("/tablet_balancer/ext_memory_moves");
    ProfilingCounters_.TabletMerges = profiler.Counter("/tablet_balancer/tablet_merges");
    ProfilingCounters_.TabletCellMoves = profiler.Counter("/tablet_tracker/tablet_cell_moves");
    ProfilingCounters_.PeerAssignment = profiler.Counter("/tablet_tracker/peer_assignment");
    ProfilingCounters_.LeaderSmoothings = profiler.Counter("/tablet_tracker/leader_smoothings");
}

std::string TCellBundleProfilingCounters::FormatErrorCode(TErrorCode errorCode)
{
    auto enumValue = static_cast<EErrorCode>(static_cast<int>(errorCode));
    if (TEnumTraits<EErrorCode>::FindLiteralByValue(enumValue) != nullptr) {
        return Format("%lv", enumValue);
    }
    return Format("%lv", NYT::EErrorCode::Generic);
}

TCounter& TCellBundleProfilingCounters::GetLeaderReassignment(TErrorCode errorCode)
{
    auto it = LeaderReassignment.find(errorCode);
    if (it == LeaderReassignment.end()) {
        it = LeaderReassignment.emplace(
            errorCode,
            Profiler.WithTag("error_code", FormatErrorCode(errorCode))
                .Counter("/tablet_tracker/leader_reassignment")).first;
    }
    return it->second;
}

TCounter& TCellBundleProfilingCounters::GetPeerRevocation(TErrorCode errorCode)
{
    auto it = PeerRevocation.find(errorCode);
    if (it == PeerRevocation.end()) {
        it = PeerRevocation.emplace(
            errorCode,
            Profiler.WithTag("error_code", FormatErrorCode(errorCode))
                .Counter("/tablet_tracker/peer_revocation")).first;
    }
    return it->second;
}

TArea* TCellBundle::GetAreaOrThrow(const std::string& name)
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
