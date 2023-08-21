#pragma once

#include "public.h"

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| if the current fiber currently handles a mutation returned as
//! a boomerang.
bool IsBoomerangMutation();

////////////////////////////////////////////////////////////////////////////////

//! A device for tracking "boomerang waves".
/*!
 *  A "boomerang" is a mutating request that is routed through a transaction
 *  coordinator so that, upon its return, all transactions required for its
 *  execution have been replicated to that cell.
 *
 *  However, since a request may require more than one transaction to be
 *  replicated and these transactions may happen to be coordinated by different
 *  cells, more than one boomerang may be launched for the same mutating
 *  request. Such set of boomerangs is called a "boomerang wave". To ensure that
 *  request execution only happens after *all* transactions have been replicated,
 *  it is performed upon return of the *last* boomerang in a wave.
 *
 *  Thread affinity: AutomatonThread
 */
class TBoomerangTracker
    : public TRefCounted
{
public:
    explicit TBoomerangTracker(NCellMaster::TBootstrap* bootstrap);
    ~TBoomerangTracker();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Start();
    void Stop();
    void Clear();

    //! Marks a boomerang as returned and, if it's a last boomerang in its wave,
    //! applies corresponding mutation.
    void ProcessReturnedBoomerang(NProto::TReqReturnBoomerang* request);

    //! Evicts boomerang waves that take too long to return.
    /*!
     *  Called within a corresponding (regularly scheduled) mutation.
     */
    void RemoveStuckBoomerangWaves(NProto::TReqRemoveStuckBoomerangWaves* request);

private:
    struct TBoomerangWaveDescriptor
    {
        TBoomerangWaveDescriptor() = default;
        TBoomerangWaveDescriptor(TInstant firstEncounterTime, int size);

        TInstant FirstEncounterTime;
        int Size = 0;
        int ReturnedBoomerangCount = 0;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);
    };

    NCellMaster::TBootstrap* const Bootstrap_;
    THashMap<TBoomerangWaveId, TBoomerangWaveDescriptor> InFlightBoomerangWaves_;
    // This is essentially a multimap, but with deterministic order of both keys and values.
    std::set<std::pair<TInstant, TBoomerangWaveId>> BoomerangWavesByTime_;
    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;
    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_;

    void OnCheck();

    TBoomerangWaveDescriptor* GetOrCreateBoomerangWaveDescriptor(TBoomerangWaveId waveId, int waveSize);
    void RemoveBoomerangWave(TBoomerangWaveId waveId);

    void ApplyBoomerangMutation(NProto::TReqReturnBoomerang* request);

    const TBoomerangTrackerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);
};

DEFINE_REFCOUNTED_TYPE(TBoomerangTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
