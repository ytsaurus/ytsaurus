#pragma once

#include "public.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct IWorldInitializer
    : public TRefCounted
{
    //! Returns |true| if the cluster is initialized.
    /*!
     *. \note Thread affinity: any
     */
    virtual bool IsInitialized() = 0;

    //! Checks if the cluster is initialized. Throws if not yet.
    /*!
     *. \note Thread affinity: any
     */
    virtual void ValidateInitialized() = 0;

    //! Returns |true| if provision lock is active.
    /*!
     * \note
    *  May only be called on the primary cell.
     * Thread affinity: AutomatonThread
     */
    virtual bool HasProvisionLock() = 0;
};

DEFINE_REFCOUNTED_TYPE(IWorldInitializer)

////////////////////////////////////////////////////////////////////////////////

IWorldInitializerPtr CreateWorldInitializer(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
