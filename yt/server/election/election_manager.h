#pragma once

#include "public.h"

#include <core/actions/cancelable_context.h>

#include <core/ytree/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

struct IElectionCallbacks
    : public virtual TRefCounted
{
    virtual void OnStartLeading() = 0;
    virtual void OnStopLeading() = 0;
    virtual void OnStartFollowing() = 0;
    virtual void OnStopFollowing() = 0;

    virtual TPeerPriority GetPriority() = 0;
    virtual Stroka FormatPriority(TPeerPriority priority) = 0;
};

DEFINE_REFCOUNTED_TYPE(IElectionCallbacks)

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
    TPeerId LeaderId = InvalidPeerId;
    TEpochId EpochId;
    TInstant StartTime;
    TCancelableContextPtr CancelableContext = New<TCancelableContext>();
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

class TElectionManager
    : public TRefCounted
{
public:
    TElectionManager(
        TElectionManagerConfigPtr config,
        TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker,
        IElectionCallbacksPtr electionCallbacks);
    ~TElectionManager();

    void Start();
    void Stop();

    NRpc::IServicePtr GetRpcService();

    NYTree::TYsonProducer GetMonitoringProducer();

    TEpochContextPtr GetEpochContext();

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    TImplPtr Impl;

};

DEFINE_REFCOUNTED_TYPE(TElectionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
