#pragma once

#include "public.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Initializes the world, that is, creates all system nodes in Cypress. 
class TWorldInitializer
    : public TRefCounted
{
public:
    TWorldInitializer(TBootstrap* bootstrap);

    ~TWorldInitializer();

    //! Returns True iff the world is already initialized.
    bool IsInitialized() const;

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

