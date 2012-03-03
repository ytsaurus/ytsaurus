#pragma once

#include <ytlib/cell_master/bootstrap.h>

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
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

