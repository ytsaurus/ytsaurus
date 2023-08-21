#include "mutation_idempotizer.h"

#include "private.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/yt/server/lib/hydra_common/mutation_context.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NObjectServer {

using namespace NConcurrency;
using namespace NCellMaster;
using namespace NHydra;
using namespace NRpc;

///////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

TMutationIdempotizer::TMutationIdempotizer(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    YT_VERIFY(configManager);
    configManager->SubscribeConfigChanged(BIND(&TMutationIdempotizer::OnDynamicConfigChanged, MakeWeak(this)));
}

TMutationIdempotizer::~TMutationIdempotizer() = default;

void TMutationIdempotizer::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, FinishedMutationsByTime_);
}

void TMutationIdempotizer::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, FinishedMutationsByTime_);

    FinishedMutations_.reserve(FinishedMutationsByTime_.size());
    for (auto [time, mutationId] : FinishedMutationsByTime_) {
        FinishedMutations_.insert(mutationId);
    }
}

void TMutationIdempotizer::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::ObjectManager),
        BIND(&TMutationIdempotizer::OnCheck, MakeWeak(this)));
    CheckExecutor_->Start();
}

void TMutationIdempotizer::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CheckExecutor_.Reset();
}

void TMutationIdempotizer::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    FinishedMutations_.clear();
    FinishedMutationsByTime_.clear();
}

bool TMutationIdempotizer::IsMutationApplied(TMutationId id) const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!Enabled_) {
        return false;
    }

    return FinishedMutations_.contains(id);
}

void TMutationIdempotizer::SetMutationApplied(TMutationId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!Enabled_) {
        return;
    }

    auto* mutationContext = GetCurrentMutationContext();
    YT_VERIFY(mutationContext);

    YT_VERIFY(FinishedMutations_.insert(id).second);
    auto now = mutationContext->GetTimestamp();
    FinishedMutationsByTime_.emplace(now, id);
}

void TMutationIdempotizer::RemoveExpiredMutations()
{
    YT_LOG_DEBUG("Started removing expired recently applied mutation IDs");

    const auto* mutationContext = GetCurrentMutationContext();
    YT_VERIFY(mutationContext);
    auto now = mutationContext->GetTimestamp();
    auto expirationTime = GetDynamicConfig()->ExpirationTime;

    const auto maxRemovalCount = GetDynamicConfig()->MaxExpiredMutationIdRemovalsPerCommit;

    auto it = FinishedMutationsByTime_.begin();
    auto ite = FinishedMutationsByTime_.end();
    auto mutationsRemoved = 0;
    for ( ; mutationsRemoved < maxRemovalCount && it != ite; ++mutationsRemoved, ++it) {
        if (it->first + expirationTime >= now) {
            break;
        }
        FinishedMutations_.erase(it->second);
    }

    FinishedMutationsByTime_.erase(FinishedMutationsByTime_.begin(), it);

    YT_LOG_DEBUG("Finished removing expired recently applied mutation IDs (MutationsRemoved: %v)",
        mutationsRemoved);
}

i64 TMutationIdempotizer::RecentlyFinishedMutationCount() const
{
    return FinishedMutations_.size();
}

void TMutationIdempotizer::OnCheck()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActiveLeader()) {
        return;
    }

    NProto::TReqRemoveExpiredRecentlyAppliedMutationIds request;
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

const TMutationIdempotizerConfigPtr& TMutationIdempotizer::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ObjectManager->MutationIdempotizer;
}

void TMutationIdempotizer::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    Enabled_ = GetDynamicConfig()->Enabled;
    if (CheckExecutor_) {
        CheckExecutor_->SetPeriod(Enabled_
            ? std::make_optional(GetDynamicConfig()->ExpirationCheckPeriod)
            : std::nullopt);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
