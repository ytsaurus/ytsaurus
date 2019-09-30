#pragma once

#include "public.h"

#include <yt/client/hydra/version.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TEpochHistoryManager
    : public TRefCounted
{
public:
    explicit TEpochHistoryManager(TBootstrap* bootstrap);
    ~TEpochHistoryManager();

    std::pair<TInstant, TInstant> GetEstimatedMutationTime(NHydra::TVersion version) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEpochHistoryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

