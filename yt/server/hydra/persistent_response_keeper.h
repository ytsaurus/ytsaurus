#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A keeper instance whose state is persisted by Hydra.
/*!
 *  \note
 *  IResponseKeeper::EndRequest only remembers the response if Hydra mutation
 *  is in progress.
 */
class TPersistentResponseKeeper
    : public TRefCounted
{
public:
    TPersistentResponseKeeper(
        NRpc::TResponseKeeperConfigPtr config,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr compositeAutomaton,
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    ~TPersistentResponseKeeper();

    NRpc::IResponseKeeperPtr GetResponseKeeper();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TPersistentResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
