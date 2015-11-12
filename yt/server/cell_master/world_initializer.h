#pragma once

#include "public.h"

#include <yt/server/hydra/mutation.h>

#include <yt/ytlib/election/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TWorldInitializer
    : public TRefCounted
{
public:
    TWorldInitializer(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~TWorldInitializer();

    //! Returns |true| if the cluster is initialized.
    bool CheckInitialized();

    //! Returns |true| if provision lock is active.
    bool CheckProvisionLock();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TWorldInitializer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
