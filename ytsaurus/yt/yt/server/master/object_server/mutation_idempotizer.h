#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NObjectServer {

///////////////////////////////////////////////////////////////////////////////

//! Stores the set of recently applied mutations.
/*!
 *  This makes mutations idempotent: applying the same mutation twice becomes
 *  safe (because the second application will be a no-op).
 *
 *  The set of applied mutations is stored "persistently" (i.e. it's part of the
 *  persistent automaton state), but is regularly cleaned up.
 *
 *  Cf. NYT::NRpc::NResponseKeeper.
 */
class TMutationIdempotizer
    : public TRefCounted
{
public:
    explicit TMutationIdempotizer(NCellMaster::TBootstrap* bootstrap);
    ~TMutationIdempotizer();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Start();
    void Stop();
    void Clear();

    bool IsMutationApplied(NRpc::TMutationId id) const;
    void SetMutationApplied(NRpc::TMutationId id);

    void RemoveExpiredMutations();

    i64 RecentlyFinishedMutationCount() const;

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    NCellMaster::TBootstrap* const Bootstrap_;
    bool Enabled_ = true;

    THashSet<NRpc::TMutationId> FinishedMutations_;
    // This is essentially a multimap, but with deterministic order of both keys and values.
    std::set<std::pair<TInstant, NRpc::TMutationId>> FinishedMutationsByTime_;

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    void OnCheck();
    const TMutationIdempotizerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);
};

DEFINE_REFCOUNTED_TYPE(TMutationIdempotizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
