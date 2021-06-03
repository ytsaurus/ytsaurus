#pragma once

#include "config.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A data structure for keeping and updating a list of participants that are alive
//! in the certain group (defined by its Cypress directory path) with their attributes.
class TDiscovery
    : public virtual TRefCounted
{
public:
    static constexpr int Version = 1;

    TDiscovery(
        TDiscoveryConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        NLogging::TLogger logger = {});

    //! Make this participant exposed to the group.
    //! It doesn't update the stored list of participants,
    //! but will add {name, attributes} in every result of List().
    TFuture<void> Enter(TString name, NYTree::IAttributeDictionaryPtr attributes);
    //! Make this participant unexposed to the group.
    //! It doesn't update the stored list of participants.
    TFuture<void> Leave();

    //! Return the list of participants stored in data structure.
    THashMap<TString, NYTree::IAttributeDictionaryPtr> List(bool includeBanned = false) const;
    //! Temporary add |name| to the ban list (timeout is specified in discovery config).
    //! Instances from the ban list are excluded from the list of available participants.
    void Ban(const TString& name);
    void Ban(const std::vector<TString>& names);
    //! Remove |name| from the ban list.
    void Unban(const TString& name);
    void Unban(const::std::vector<TString>& names);

    //! Force update the list of participants if stored data is older than |maxDivergency|.
    //! Returns a future that becomes set when data is up to date.
    TFuture<void> UpdateList(TDuration maxDivergency = TDuration::Zero());

    //! Start updating the list of available participants.
    //! Returns a future that becomes set after first update.
    TFuture<void> StartPolling();
    //! Stop updating the list of available participants.
    //! Returns a future that becomes set after stopping PeriodicExecutor.
    TFuture<void> StopPolling();

    //! Return weight of TDiscovery in units. Can be used in Cache.
    i64 GetWeight();

private:
    TDiscoveryConfigPtr Config_;
    NApi::IClientPtr Client_;
    IInvokerPtr Invoker_;
    THashMap<TString, NYTree::IAttributeDictionaryPtr> List_;
    THashMap<TString, TInstant> BannedUntil_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    NApi::TListNodeOptions ListOptions_;
    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, Lock_);
    NApi::ITransactionPtr Transaction_;
    const NLogging::TLogger Logger;
    std::optional<std::pair<TString, NYTree::IAttributeDictionaryPtr>> NameAndAttributes_;
    TFuture<void> ScheduledForceUpdate_;
    TInstant LastUpdate_;
    TCallback<void(void)> TransactionRestorer_;
    int Epoch_;

    void DoEnter(TString name, NYTree::IAttributeDictionaryPtr attributes);
    void DoLeave();

    void GuardedUpdateList();
    // Same as above, but catch and discard all exceptions.
    void DoUpdateList();

    void DoCreateNode(int epoch);
    void DoLockNode(int epoch);

    void DoRestoreTransaction(int epoch);
};

DEFINE_REFCOUNTED_TYPE(TDiscovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
