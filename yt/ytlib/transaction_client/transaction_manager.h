#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/api/client.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
class TTransaction
    : public TRefCounted
{
public:
    ~TTransaction();

    //! Commits the transaction asynchronously.
    /*!
     *  Should not be called more than once.
     *
     *  \note Thread affinity: ClientThread
     */
    TAsyncError Commit(
        const NHydra::TMutationId& mutationId = NHydra::NullMutationId);

    //! Aborts the transaction asynchronously.
    /*!
     *  \note Thread affinity: any
     */
    TAsyncError Abort(
        bool force = false,
        const NHydra::TMutationId& mutationId = NHydra::NullMutationId);

    //! Detaches the transaction, i.e. stops pings.
    /*!
     *  This call does not block and does not throw.
     *  Safe to call multiple times.
     *
     *  \note Thread affinity: ClientThread
     */
    void Detach();

    //! Sends an asynchronous ping.
    /*!
     *  \note Thread affinity: any
     */
    TAsyncError Ping();


    //! Returns the transaction type.
    /*!
     *  \note Thread affinity: any
     */
    ETransactionType GetType() const;

    //! Returns the transaction id.
    /*!
     *  \note Thread affinity: any
     */
    const TTransactionId& GetId() const;

    //! Returns the transaction start timestamp.
    /*!
     *  \note Thread affinity: any
     */
    TTimestamp GetStartTimestamp() const;


    //! Called to mark a given cell as a transaction participant.
    //! Starts the corresponding transaction and returns the async result.
    /*!
     *  \note Thread affinity: ClientThread
     */
    TAsyncError AddTabletParticipant(const NElection::TCellId& cellId);


    //! Raised when the transaction is aborted.
    /*!
     *  \note Thread affinity: any
     */
    DECLARE_SIGNAL(void(), Aborted);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    friend class TTransactionManager;

    template <class T, class... As>
    friend TIntrusivePtr<T> NYT::New(As&&... args);

    static TTransactionPtr Create(TIntrusivePtr<TImpl> impl);
    explicit TTransaction(TIntrusivePtr<TImpl> impl);

};

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

//! Describes settings for a newly created transaction.
struct TTransactionStartOptions
    : public NApi::TTransactionStartOptions
{
    TTransactionStartOptions();
    TTransactionStartOptions(const TTransactionStartOptions& other) = default;
    TTransactionStartOptions(const NApi::TTransactionStartOptions& other);

    NHydra::TMutationId MutationId;
    bool EnableUncommittedAccounting;
    bool EnableStagedAccounting;
};

//! Describes settings used for attaching to existing transactions.
struct TTransactionAttachOptions
{
    explicit TTransactionAttachOptions(const TTransactionId& id);

    TTransactionId Id;
    bool AutoAbort;
    bool Ping;
    bool PingAncestors;
};

//! Controls transactions at client-side.
/*!
 *  Provides a factory for all client-side transactions.
 *  Keeps track of all active transactions and sends pings to master servers periodically.
 *
 * /note Thread affinity: any
 */
class TTransactionManager
    : public TRefCounted
{
public:
    //! Initializes an instance.
    /*!
     * \param config A configuration.
     * \param channel A channel used for communicating with masters.
     */
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        NObjectClient::TCellTag cellTag,
        const NHive::TCellId& cellId,
        NRpc::IChannelPtr masterChannel,
        ITimestampProviderPtr timestampProvider,
        NHive::TCellDirectoryPtr cellDirectory);

    ~TTransactionManager();

    //! Asynchronously starts a new transaction.
    /*!
     *  If |options.Ping| is |true| then transaction's lease will be renewed periodically.
     *
     *  If |options.PingAncestors| is |true| then the above renewal will also apply to all
     *  ancestor transactions.
     */
    TFuture<TErrorOr<TTransactionPtr>> Start(
        ETransactionType type,
        const TTransactionStartOptions& options);
    
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
    TTransactionPtr Attach(const TTransactionAttachOptions& options);

    //! Asynchronously aborts all active transactions.
    void AbortAll();

private:
    friend class TTransaction;
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
