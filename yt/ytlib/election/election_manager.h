#pragma once

#include "public.h"

#include <ytlib/actions/cancelable_context.h>

#include <ytlib/yson/public.h>

#include <ytlib/rpc/public.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TIntrinsicRefCounted
{
    TEpochContext();

    TPeerId LeaderId;
    TEpochId EpochId;
    TInstant StartTime;
    TCancelableContextPtr CancelableContext;

};

////////////////////////////////////////////////////////////////////////////////

class TElectionManager
    : public TRefCounted
{
public:
    TElectionManager(
        TElectionManagerConfigPtr config,
        TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker,
        IElectionCallbacksPtr electionCallbacks,
        NRpc::IServerPtr rpcServer);

    ~TElectionManager();

    void Start();
    void Stop();
    void Restart();

    void GetMonitoringInfo(NYson::IYsonConsumer* consumer);

    TEpochContextPtr GetEpochContext();

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    TImplPtr Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
