#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <core/ytree/public.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/hive/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETransactionType,
    (Master) // accepted by both masters and tablets
    (Tablet) // accepted by tablets only
);

//! Describes settings for a newly created transaction.
struct TTransactionStartOptions
{
    TTransactionStartOptions();

    TNullable<TDuration> Timeout;
    NHydra::TMutationId MutationId;
    TTransactionId ParentId;
    bool AutoAbort;
    bool Ping;
    bool PingAncestors;
    bool EnableUncommittedAccounting;
    bool EnableStagedAccounting;
    std::shared_ptr<NYTree::IAttributeDictionary> Attributes; // to make the type copyable
    ETransactionType Type;
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
    : public virtual TRefCounted
{
public:
    //! Initializes an instance.
    /*!
     * \param config A configuration.
     * \param channel A channel used for communicating with masters.
     */
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        const NHive::TCellGuid& masterCellGuid,
        NRpc::IChannelPtr masterChannel,
        NHive::ITimestampProviderPtr timestampProvider,
        NHive::TCellDirectoryPtr cellDirectory);

    //! Starts a new transaction.
    /*!
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
    TFuture<TErrorOr<ITransactionPtr>> AsyncStart(const TTransactionStartOptions& options);
    
    //! Synchronous version of #AsyncStart.
    ITransactionPtr Start(const TTransactionStartOptions& options);

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
    ITransactionPtr Attach(const TTransactionAttachOptions& options);

    //! Aborts all active transactions.
    void AsyncAbortAll();

private:
    class TTransaction;
    typedef TIntrusivePtr<TTransaction> TTransactionPtr;

    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    TImplPtr Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
