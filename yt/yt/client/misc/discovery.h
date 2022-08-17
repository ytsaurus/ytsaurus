#pragma once

#include "config.h"

#include <yt/yt/client/api/transaction.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An interaface for keeping and updating a list of participants that are alive
//! in the certain group with their attributes.
struct IDiscovery
    : public virtual TRefCounted
{
    //! Make this participant exposed to the group.
    //! It doesn't update the stored list of participants,
    //! but will add {name, attributes} in every result of List().
    virtual TFuture<void> Enter(TString name, NYTree::IAttributeDictionaryPtr attributes) = 0;
    //! Make this participant unexposed to the group.
    //! It doesn't update the stored list of participants.
    virtual TFuture<void> Leave() = 0;

    //! Return the list of participants stored in data structure.
    virtual THashMap<TString, NYTree::IAttributeDictionaryPtr> List(bool includeBanned = false) const = 0;
    //! Temporary add |name| to the ban list (timeout is specified in discovery config).
    //! Instances from the ban list are excluded from the list of available participants.
    virtual void Ban(const TString& name) = 0;
    virtual void Ban(const std::vector<TString>& names) = 0;
    //! Remove |name| from the ban list.
    virtual void Unban(const TString& name) = 0;
    virtual void Unban(const::std::vector<TString>& names) = 0;

    //! Force update the list of participants if stored data is older than |maxDivergency|.
    //! Returns a future that becomes set when data is up to date.
    virtual TFuture<void> UpdateList(TDuration maxDivergency = TDuration::Zero()) = 0;

    //! Start updating the list of available participants.
    //! Returns a future that becomes set after first update.
    virtual TFuture<void> StartPolling() = 0;
    //! Stop updating the list of available participants.
    //! Returns a future that becomes set after stopping PeriodicExecutor.
    virtual TFuture<void> StopPolling() = 0;

    //! Returns a version of the discovery.
    virtual int Version() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiscovery)

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryV1(
    TDiscoveryConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

class TDiscovery
    : public IDiscovery
{
public:
    TDiscovery(
        TDiscoveryConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        NLogging::TLogger logger = {});

    TFuture<void> Enter(TString name, NYTree::IAttributeDictionaryPtr attributes) override;
    TFuture<void> Leave() override;

    THashMap<TString, NYTree::IAttributeDictionaryPtr> List(bool includeBanned = false) const override;
    void Ban(const TString& name) override;
    void Ban(const std::vector<TString>& names) override;
    void Unban(const TString& name) override;
    void Unban(const::std::vector<TString>& names) override;

    TFuture<void> UpdateList(TDuration maxDivergency = TDuration::Zero()) override;

    TFuture<void> StartPolling() override;
    TFuture<void> StopPolling() override;

    int Version() const override;

private:
    TDiscoveryConfigPtr Config_;
    NApi::IClientPtr Client_;
    IInvokerPtr Invoker_;
    THashMap<TString, NYTree::IAttributeDictionaryPtr> List_;
    THashMap<TString, TInstant> BannedUntil_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    NApi::TListNodeOptions ListOptions_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    NApi::ITransactionPtr Transaction_;
    const NLogging::TLogger Logger;
    std::optional<std::pair<TString, NYTree::IAttributeDictionaryPtr>> NameAndAttributes_;
    TFuture<void> ScheduledForceUpdate_;
    TInstant LastUpdate_;
    NApi::ITransaction::TAbortedHandler TransactionAbortedHandler_;
    int Epoch_;

    void DoEnter(TString name, NYTree::IAttributeDictionaryPtr attributes);
    void DoLeave();

    void GuardedUpdateList();
    // Same as above, but catch and discard all exceptions.
    void DoUpdateList();

    void DoCreateNode(int epoch);
    void DoLockNode(int epoch);

    void DoRestoreTransaction(int epoch, const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TDiscovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
