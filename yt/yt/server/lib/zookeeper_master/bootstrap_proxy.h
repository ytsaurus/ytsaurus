#pragma once

#include "public.h"

namespace NYT::NZookeeperMaster {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrapProxy
{
    virtual ~IBootstrapProxy() = default;

    //! Checks if it is allowed to read automaton state from current context.
    //! Read is allowed either from automaton thread or from any thread when
    //! automaton is locked.
    virtual void VerifyPersistentStateRead() = 0;

    //! Returns automaton for zookeeper.
    virtual NHydra::TCompositeAutomatonPtr GetAutomaton() const = 0;

    //! Returns invoker corresponding to automaton thread.
    virtual IInvokerPtr GetAutomatonInvoker() const = 0;

    //! Returns Hydra manager corresponding to automaton.
    virtual NHydra::IHydraManagerPtr GetHydraManager() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
