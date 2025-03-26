#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

using TTransactionStartOptions = NApi::TTransactionStartOptions;
using TTransactionAttachOptions = NApi::TTransactionAttachOptions;
using TTransactionCommitOptions = NApi::TTransactionCommitOptions;
using TTransactionCommitResult = NApi::TTransactionCommitResult;
using TTransactionAbortOptions = NApi::TTransactionAbortOptions;
using TTransactionPingOptions = NApi::TTransactionPingOptions;

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
/*!
 *  /note Thread affinity: any
 */
class TTransaction
    : public TRefCounted
{
public:
    ~TTransaction();

    //! Commits the transaction asynchronously.
    /*!
     *  Should not be called more than once.
     */
    TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options = TTransactionCommitOptions());

    //! Aborts the transaction asynchronously.
    TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions());

    //! Detaches the transaction, i.e. stops pings.
    /*!
     *  This call does not block and does not throw.
     *  Safe to call multiple times.
     */
    void Detach();

    //! Sends an asynchronous ping.
    TFuture<void> Ping(const TTransactionPingOptions& options = {});

    //! Returns the transaction type.
    ETransactionType GetType() const;

    //! Returns the transaction id.
    TTransactionId GetId() const;

    //! Returns the transaction start timestamp.
    /*!
     *  For non-atomic transactions this timestamp is client-generated (i.e. approximate).
     */
    TTimestamp GetStartTimestamp() const;

    //! Returns the transaction atomicity mode.
    EAtomicity GetAtomicity() const;

    //! Returns the transaction durability mode.
    EDurability GetDurability() const;

    //! Returns the transaction timeout.
    TDuration GetTimeout() const;

    //! Returns the transaction prerequisite transaction ids.
    const std::vector<TTransactionId>& GetPrerequisiteTransactionIds() const;

    //! Returns the tags of the cells this transaction is replicated to.
    //! Only supported for master transactions that are created locally, not attached.
    NObjectClient::TCellTagList GetReplicatedToCellTags() const;

    //! Once a participant is registered, it will be pinged.
    void RegisterParticipant(NObjectClient::TCellId cellId);

    //! Choose transaction coordinator.
    void ChooseCoordinator(const TTransactionCommitOptions& options);

    //! Check that all participants are healthy.
    TFuture<void> ValidateNoDownedParticipants();

    //! Raised when the transaction is committed.
    DECLARE_SIGNAL(NApi::ITransaction::TCommittedHandlerSignature, Committed);

    //! Raised when the transaction is aborted.
    DECLARE_SIGNAL(NApi::ITransaction::TAbortedHandlerSignature, Aborted);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;

    friend class TTransactionManager;
    DECLARE_NEW_FRIEND()

    static TTransactionPtr Create(TImplPtr impl);
    explicit TTransaction(TImplPtr impl);

};

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

//! Controls transactions at client-side.
/*!
 *  Provides a factory for all client-side transactions.
 *  Keeps track of all active transactions and sends pings to master servers periodically.
 *
 *  /note Thread affinity: any
 */
class TTransactionManager
    : public TRefCounted
{
public:
    TTransactionManager(
        NApi::NNative::IConnectionPtr connection,
        const std::string& user);

    ~TTransactionManager();

    //! Asynchronously starts a new transaction.
    /*!
     *  If |options.Ping| is |true| then transaction's lease will be renewed periodically.
     *
     *  If |options.PingAncestors| is |true| then the above renewal will also apply to all
     *  ancestor transactions.
     */
    TFuture<TTransactionPtr> Start(
        ETransactionType type,
        const NApi::TTransactionStartOptions& options = {});

    //! Attaches to an existing transaction.
    /*!
     *  If |options.AutoAbort| is True then the transaction will be aborted
     *  (if not already committed) at the end of its lifetime.
     *
     *  If |options.Ping| is True then Transaction Manager will be renewing
     *  the lease of this transaction.
     *
     *  If |options.PingAncestors| is True then Transaction Manager will be renewing
     *  the leases of all ancestors of this transaction.
     *
     *  \note
     *  This call does not block.
     */
    TTransactionPtr Attach(
        TTransactionId id,
        const TTransactionAttachOptions& options = TTransactionAttachOptions());

    //! Asynchronously aborts all active transactions.
    void AbortAll();

private:
    friend class TTransaction;
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
