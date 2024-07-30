#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/small_containers/compact_flat_map.h>

namespace NYT::NTransactionServer {

///////////////////////////////////////////////////////////////////////////////

//! Used by replication sessions below for logging and identity propagation purposes.
struct TTransactionReplicationInitiatorRequestInfo
{
    NRpc::TAuthenticationIdentity Identity;
    NRpc::TRequestId RequestId;
    std::optional<int> SubrequestIndex;
};

////////////////////////////////////////////////////////////////////////////////

//! A means for taking necessary preliminary steps before a transactional
//! request may be executed (and yield consistent results).
/*!
 *  A transactional request is a request that's either expected to run within a
 *  particular transaction or specifies at least one transaction as its
 *  prerequisite (or both).
 *
 *  Executing any such request requires syncing with certain master cells and
 *  replicating those transactions that haven't yet been replicated here. A
 *  replication session does all that.
 *
 *  NB: ordinarily, these sessions should not be used directly; instead an
 *  all-in-one |RunTransactionReplicationSession| functions should be used. A
 *  direct use should be reserved for special cases like handling batch requests
 *  and minimizing automaton thread usage.
 */
class TTransactionReplicationSessionBase
    : public TRefCounted
{
public:
    //! Reinitializes this session as if it was constructed with #transactionIds
    //! in the first place. Does nothing if new transactions match old
    //! transactions. May throw if called in between epochs.
    void Reset(std::vector<TTransactionId> transactionIds);

protected:
    using TReqReplicateTransactionsPtr = NTransactionClient::TTransactionServiceProxy::TReqReplicateTransactionsPtr;
    using TRspReplicateTransactionsPtr = NTransactionClient::TTransactionServiceProxy::TRspReplicateTransactionsPtr;

    using TReplicationRequestMap = TCompactFlatMap<NObjectClient::TCellTag, TReqReplicateTransactionsPtr, 4>;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    NCellMaster::TBootstrap* const Bootstrap_;
    const std::optional<TTransactionReplicationInitiatorRequestInfo> RequestInfo_;

    const NLogging::TLogger Logger;

    std::vector<TTransactionId> AllTransactionIds_;
    TMutableRange<TTransactionId> LocalTransactionIds_;
    TRange<TTransactionId> RemoteTransactionIds_;
    TRange<TTransactionId> MirroredTransactionIds_;

    // The former contains the keys of the latter, but its calculated earlier
    // and provides deterministic order.
    NObjectClient::TCellTagList ReplicationRequestCellTags_;
    TReplicationRequestMap ReplicationRequests_;

    NObjectClient::TCellTagList UnsyncedLocalTransactionCells_;

    // COMPAT(kvk1920): remove when use of Cypress tx proxy becomes mandatory.
    // We cannot rely on ID only to distinguish mirrored Cypress transaction
    // from non-mirrored one: both mirrored Cypress and Sequoia transactions
    // have Sequoia bit in their ID and when dedicated system transaction type
    // is disabled both of them have a type "transaction". The following flag is
    // necessary just for this case.
    const bool MirroringToSequoiaEnabled_;

    // COMPAT(babenko)
    bool EnableBoomerangsIdentity_;

    TTransactionReplicationSessionBase(
        NCellMaster::TBootstrap* bootstrap,
        std::vector<TTransactionId> transactionIds,
        std::optional<TTransactionReplicationInitiatorRequestInfo> requestInfo,
        bool enableMirroringToSequoia,
        // COMPAT(babenko)
        bool enableBoomerangsIdentity);

    [[noreturn]] void LogAndThrowUnknownTransactionPresenceError(TTransactionId transactionId) const;

    NObjectClient::TCellTagList GetCellTagsToSyncWithBeforeInvocation() const;

    struct TReplicationResponse
    {
        std::vector<TFuture<TRspReplicateTransactionsPtr>> NonMirrored;
        TFuture<void> Mirrored;
    };

    TReplicationResponse DoInvokeReplicationRequests();

    virtual void ConstructReplicationRequests() = 0;
    std::vector<NRpc::TRequestId> DoConstructReplicationRequests();

    TError WrapError(TError error) const;

private:
    void Initialize();
    void SegregateMirroredTransactions();
    void InitRemoteTransactions();
    void InitReplicationRequestCellTags();

    void ValidateTransactionCellTags() const;
    //! Returns |true| if transaction may be not replicated to this cell.
    bool IsTransactionRemote(TTransactionId transactionId) const;

    // NB: This function must not be used when mirroring is disabled.
    static bool IsMirroredToSequoia(TTransactionId transactionId);
};

////////////////////////////////////////////////////////////////////////////////

//! A replication session that syncs with cells and replicates transactions and that's it.
//! Cf. TTransactionReplicationSessionWithBoomerangs
class TTransactionReplicationSessionWithoutBoomerangs
    : public TTransactionReplicationSessionBase
{
public:
    using TTransactionReplicationSessionBase::TTransactionReplicationSessionBase;

    //! Returns a future that will be set when all necessary syncs with
    //! transaction coordinator cells are done and all transactions are
    //! replicated this cell.
    /*
     *  Once the future is set, the caller is expected to proceed with the
     *  actual request execution.
     *
     *  The future returned may be set to an error if transaction replication fails.
     *
     *  May throw if called in between epochs.
     *
     *  NB: this is an all-in-one method. Alternatively, one might consider
     *  going step-by-step using the individual methods below.
     */
    TFuture<void> Run(bool syncWithUpstream);

    //! Returns a set of cells the caller must sync with either before or in
    //! parallel with replication requests.
    NObjectClient::TCellTagList GetCellTagsToSyncWithDuringInvocation() const;

    //! Invokes transaction replication requests and returns a somewhat transformed future result.
    /*!
     *  The (uppermost) future returned is only set to an error if transaction
     *  presence cache denies subscribing to transaction replication events
     *  (thus indicating that current epoch has ended). It may also be null if
     *  this session no-op (has nothing to replicate).
     *
     *  Each inner future:
     *    - is set when corresponding transaction has actually been replicated here;
     *    - may be set to an error in case replication request fails (and thus
     *      transaction replication is not guaranteed to be forthcoming).
     */
    TFuture<THashMap<TTransactionId, TFuture<void>>> InvokeReplicationRequests();

    //! Returns a set of cells to sync with after replication requests have finished.
    /*!
     *  Some replication requests may find that, on transaction coordinator
     *  cell, the transaction requested to be replicated has already been posted
     *  for replication (by another, earlier request).
     *
     *  While this is fine from the replication standpoint, it deprives us of
     *  the "implicit sync" (i.e. of being able to conclude, from transaction
     *  appearing in this cell, that a full hive queue flush has happened). In
     *  that case, manual sync with tx coordinator cell is required.
     */
    NObjectClient::TCellTagList GetCellTagsToSyncWithAfterInvocation() const;

private:
    void ConstructReplicationRequests() override;

    NObjectClient::TCellTagList UnsyncedRemoteTransactionCells_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionReplicationSessionWithoutBoomerangs)

////////////////////////////////////////////////////////////////////////////////

//! A replication session that, for a mutating request, in addition to syncing
//! with cells and replicating transactions, also attempts to optimize the
//! latency of the following mutation.
/*!
 *  This is done by launching so-called "boomerang mutations". Such a mutation
 *  is sent to a transaction coordinator cell along with transaction replication
 *  request. The coordinator is then expected to send it back (after posting
 *  transaction replication requests). Once it makes it back here, the boomerang
 *  mutation is applied.
 *
 *  All this effort is made just to save a single Hive queue flush.
 *
 *  Cf. TTransactionReplicationSessionWithoutBoomerangs
 */
class TTransactionReplicationSessionWithBoomerangs
    : public TTransactionReplicationSessionBase
{
public:
    //! Constructs a replication session with boomerang mutation support.
    /*!
     *  If #mutation is null here, it should be provided later via #SetMutation
     *  before replication requests are invoked.
     *
     *  May throw if called in between epochs.
     */
    using TTransactionReplicationSessionBase::TTransactionReplicationSessionBase;

    void SetMutation(std::unique_ptr<NHydra::TMutation> mutation);

    //! Returns a future that will be set when all necessary syncs with
    //! transaction coordinator cells are done, all transactions are
    //! replicated this cell, and the mutation has been applied.
    /*!
     *  The future returned may be set to an error if transaction replication
     *  fails (among other usual reasons).
     *
     *  May throw if called in between epochs.
     */
    TFuture<void> Run(bool syncWithUpstream, const NRpc::IServiceContextPtr& context);

    //! Returns a set of cells the caller must sync with before replication requests.
    /*!
     *  Since it's impossible to predict (or control) how fast a boomerang
     *  returns and its mutations applies, syncs must occur strictly before
     *  replication requests.
     *
     *  NB: may be called even when no mutation has been provided to this
     *  session yet.
     */
    NObjectClient::TCellTagList GetCellTagsToSyncWithBeforeInvocation() const;

    //! Invokes transaction replication requests accompanied by boomerang
    //! mutations and returns a future that will be set when that mutation gets
    //! back here and is applied.
    /*
     *  NB: May commit the mutation right away if there're no transactions to
     *  replicate. Therefore, never returns null future.
     */
    TFuture<NHydra::TMutationResponse> InvokeReplicationRequests(std::optional<TDuration> timeout);

    TFuture<NHydra::TMutationResponse> InvokeReplicationRequestsOffloaded(std::optional<TDuration> timeout);

private:
    std::unique_ptr<NHydra::TMutation> Mutation_;

    void ConstructReplicationRequests() override;

    TFuture<TSharedRefArray> BeginRequestInResponseKeeper();
    TFuture<TSharedRefArray> FindRequestInResponseKeeper();
    void EndRequestInResponseKeeper(const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TTransactionReplicationSessionWithBoomerangs)

////////////////////////////////////////////////////////////////////////////////

//! Returns a future that will be set when all necessary preliminary steps for
//! executing a transactional requests are done.
/*!
 *  The future returned may be set to an error if transaction replication fails.
 *  May return null future if there's nothing to do.
 *  May throw if called in between epochs.
 */
TFuture<void> RunTransactionReplicationSession(
    bool syncWithUpstream,
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    bool enableMirroringToSequoia,
    bool enableBoomerangsIdentity);

//! Returns a future that will set when the provided mutation has been applied
//!  (after all necessary preliminary steps for applying it has been taken).
/*!
 *  The context is also replied upon mutation application.
 *
 *  May throw if called in between epochs.
 */
void RunTransactionReplicationSessionAndReply(
    bool syncWithUpstream,
    NCellMaster::TBootstrap* bootstrap,
    std::vector<TTransactionId> transactionIds,
    const NRpc::IServiceContextPtr& context,
    std::unique_ptr<NHydra::TMutation> mutation,
    bool enableMutationBoomerangs,
    bool enableMirroringToSequoia,
    bool enableBoomerangsIdentity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
