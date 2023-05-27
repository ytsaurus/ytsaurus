#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

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
    DECLARE_INTERFACE_SIGNAL(void(
        TAvenueEndpointId endpointId,
        TCellId newCellId),
        EndpointUpdated);
};

DEFINE_REFCOUNTED_TYPE(IAvenueDirectory)

////////////////////////////////////////////////////////////////////////////////

//! Direct implementation of the IAvenueDirectory interface.
/*
 *  Thread affinity: single
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
};

DEFINE_REFCOUNTED_TYPE(TSimpleAvenueDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
