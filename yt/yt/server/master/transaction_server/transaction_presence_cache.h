#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTransactionServer {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionPresence,
    ((Unknown)         (-1)) // Probably epoch ended.
    ((None)             (0))
    ((Replicated)       (1))
    ((RecentlyFinished) (2))
);

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe way of checking if a particular transaction has been
//! replicated to this cell or not.
class TTransactionPresenceCache
    : public TRefCounted
{
public:
    explicit TTransactionPresenceCache(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();
    void Clear();

    //! Returns a state of a transaction in this cell.
    /*!
     *  Thread affinity: any
     */
    ETransactionPresence GetTransactionPresence(TTransactionId transactionId) const;

    //! Returns a future that will be set when the specified transaction gets
    //! replicated to this cell.
    /*!
     *  May return null future if this peer is in between epochs at the moment.
     *
     *  The future will never be set if transaction replication never happens.
     *  Indeed, this will even leak a corresponding promise - it will never be
     *  removed from cache's internal state.
     *
     *  The corollaries are:
     *    - only subscribe once absolutely certain the replication is forthcoming;
     *    - just to be on the safe side, consider using #TFuture::WithTimeout.
     *
     *  NB: THE FUTURE IS SET FROM THE AUTOMATON THREAD!
     *  Be sure to keep subscribers light or use .AsyncVia().
     *
     *  Thread affinity: any
     */
    TFuture<void> SubscribeRemoteTransactionReplicated(TTransactionId transactionId);

    //! Returns the number of transactions replicated to this cell. This
    //! includes recently finished transactions.
    /*!
     *  Thread affinity: AutomatonThread
     */
    i64 GetReplicatedTransactionCount() const;

    //! Returns the number transactions that had been replicated here and have
    //! recently finished.
    /*!
     *  Thread affinity: AutomatonThread
     */
    i64 GetRecentlyFinishedTransactionCount() const;

    //! Returns the number of transactions that are awaited to be replicated here by some
    //! subscribers.
    /*!
     *  NB: this is the number of *transactions*, not subscribers.
     *
     *  Thread affinity: AutomatonThread
     */
    i64 GetSubscribedRemoteTransactionReplicationCount() const;

    //! Marks a transaction as replicated to this cell.
    /*!
     *  Thread affinity: AutomatonThread
     */
    void SetTransactionReplicated(TTransactionId transactionId);

    //! Marks a transaction as recently finished.
    /*!
     *  Thread affinity: AutomatonThread
     */
    void SetTransactionRecentlyFinished(TTransactionId transactionId);

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    using TTransactionPresenceMap = TConcurrentHashMap<TTransactionId, ETransactionPresence, 256, NThreading::TSpinLock>;
    using TTransactionReplicationSubscriptionMap = TConcurrentHashMap<TTransactionId, TPromise<void>, 256, NThreading::TSpinLock>;

    NCellMaster::TBootstrap* const Bootstrap_;

    std::atomic<bool> Started_ = false;

    TTransactionPresenceMap ReplicatedTransactions_;
    i64 ReplicatedTransactionCount_ = 0; // Concurrent hashmap doesn't support size().
    std::map<TInstant, TTransactionId> RecentlyFinishedTransactions_;
    TTransactionReplicationSubscriptionMap TransactionReplicationSubscriptions_;
    std::atomic<i64> SubscribedRemoteTransactionReplicationCount_ = 0;

    NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    const TCallback<void()> EvictionCallback_;
    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_;

    const IInvokerPtr AutomatonInvoker_;

    void OnEviction();

    const TTransactionPresenceCacheConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);

    void NotifyRemoteTransactionReplicated(TTransactionId transactionId);
};

DEFINE_REFCOUNTED_TYPE(TTransactionPresenceCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
