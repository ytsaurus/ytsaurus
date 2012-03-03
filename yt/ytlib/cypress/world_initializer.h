#pragma once

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

//! Initializes the world, that is, creates all system nodes in Cypress. 
class TWorldInitializer
    : public TRefCounted
{
public:
    TWorldInitializer(NCellMaster::TBootstrap* bootstrap);

    //! Returns True iff the world is already initialized.
    /*!
     *  Used to protected the cell from access before the initialization is complete.
     */
    bool IsInitialized() const;

private:
    NCellMaster::TBootstrap* Bootstrap;
    TPeriodicInvoker::TPtr PeriodicInvoker;

    void OnCheck();
    bool CanInitialize() const;
    void Initialize();
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

