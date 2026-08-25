#pragma once

#include "public.h"

#include <util/generic/hash.h>

#include <optional>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Deterministic, demand-aware weighted class arbiter.
//! Request queues and token-bucket waits are intentionally owned by the bucket.
class TWeightedFairQueueScheduler
{
public:
    explicit TWeightedFairQueueScheduler(
        const THashMap<TQuotaClassId, double>& classWeights = {},
        double renormalizationThreshold = 1e12);

    void Reconfigure(const THashMap<TQuotaClassId, double>& classWeights);

    bool IsAccepting(const TQuotaClassId& classId) const;
    bool Contains(const TQuotaClassId& classId) const;
    bool IsRetired(const TQuotaClassId& classId) const;
    double GetWeight(const TQuotaClassId& classId) const;

    void Activate(const TQuotaClassId& classId, TPriority headPriority);
    void Deactivate(const TQuotaClassId& classId);
    void UpdateHeadPriority(const TQuotaClassId& classId, TPriority headPriority);

    std::optional<TQuotaClassId> SelectClass();
    void Charge(const TQuotaClassId& classId, i64 amount, double weight);
    //! Adds |delta| to the class's virtual time directly. Rolling a charge back
    //! must reuse the value that was applied, not recompute it from the current
    //! weight, which a live reconfiguration may already have changed.
    void ChargeVirtualTime(const TQuotaClassId& classId, double delta);

    void RemoveRetiredClass(const TQuotaClassId& classId);

private:
    struct TClassState
    {
        double Weight = 1.0;
        double VirtualTime = 0.0;
        TPriority HeadPriority = 0;
        bool Active = false;
        bool Accepting = true;
    };

    void MaybeReset();
    void MaybeRenormalize();

    THashMap<TQuotaClassId, TClassState> Classes_;
    double SystemVirtualTime_ = 0.0;
    const double RenormalizationThreshold_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
