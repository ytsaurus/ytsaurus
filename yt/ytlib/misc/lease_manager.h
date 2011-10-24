#pragma once

#include "../actions/action.h"
#include "../actions/invoker.h"
#include "../misc/delayed_invoker.h"

namespace NYT 
{

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager
    : public TNonCopyable
{
    struct TEntry
        : public TRefCountedBase
    {
        bool IsValid;
        TDuration Timeout;
        IAction::TPtr OnExpire;
        TDelayedInvoker::TCookie Cookie;
        TSpinLock SpinLock;

        TEntry(TDuration timeout, IAction::TPtr onExpire)
            : IsValid(true)
            , Timeout(timeout)
            , OnExpire(onExpire)
        { }
    };

public:
    typedef TIntrusivePtr<TEntry> TLease;

    static TLeaseManager* Get();

    TLease CreateLease(TDuration timeout, IAction::TPtr expiryCallback);
    bool RenewLease(TLease lease);
    bool CloseLease(TLease lease);

private:
    bool EraseLease(TLease lease);
    void ExpireLease(TLease lease);
    void InvalidateLease(TLease lease);

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
