#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct IAvenueDirectory
    : public virtual TRefCounted
{
    //! Returns the id of the cell where the endpoint is currently registered
    //! or NullObjectId if its location is unknown.
    //!
    //! This method is called by the Hive Manager only in automaton thread.
    //! However, no fiber context switches are allowed.
    virtual TCellId FindCellIdByEndpointId(TAvenueEndpointId endpointId) const = 0;

    //! Raised when the endpoint changes its location, either moving to a different
    //! cell or losing its cell altogether.
    //!
    //! Cell mailbox must be already created when the method is called.
    DECLARE_INTERFACE_SIGNAL(void(
        TAvenueEndpointId endpointId,
        TCellId newCellId),
        EndpointUpdated);
};

DEFINE_REFCOUNTED_TYPE(IAvenueDirectory)

////////////////////////////////////////////////////////////////////////////////

//! Direct implementation of the IAvenueDirectory interface.
/*
 *  Thread affinity: any (though under a single spin lock).
 */
class TSimpleAvenueDirectory
    : public IAvenueDirectory
{
public:
    TCellId FindCellIdByEndpointId(TAvenueEndpointId endpointId) const override;

    void UpdateEndpoint(TAvenueEndpointId endpointId, TCellId cellId);

    DEFINE_SIGNAL_OVERRIDE(void(
        TAvenueEndpointId endpointId,
        TCellId newCellId),
        EndpointUpdated);

private:
    THashMap<TAvenueEndpointId, TCellId> Directory_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
};

DEFINE_REFCOUNTED_TYPE(TSimpleAvenueDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
