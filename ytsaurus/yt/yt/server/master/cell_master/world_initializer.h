#pragma once

#include "public.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct IWorldInitializer
    : public TRefCounted
{
public:
    //! Returns |true| if the cluster is initialized.
    virtual bool IsInitialized() = 0;

    //! Checks if the cluster is initialized. Throws if not.
    virtual void ValidateInitialized() = 0;

    //! Returns |true| if provision lock is active.
    //! May only be called on the primary cell.
    virtual bool HasProvisionLock() = 0;
};

DEFINE_REFCOUNTED_TYPE(IWorldInitializer)

////////////////////////////////////////////////////////////////////////////////

IWorldInitializerPtr CreateWorldInitializer(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
