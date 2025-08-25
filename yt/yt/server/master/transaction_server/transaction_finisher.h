#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTransactionFinishRequest;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////


struct TTransactionFinishRequestBase
{
    NRpc::TAuthenticationIdentity AuthenticationIdentity;
    NRpc::TMutationId MutationId;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

struct TTransactionCommitRequest
    : public TTransactionFinishRequestBase
{
    std::vector<TTransactionId> PrerequisiteTransactionIds;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

struct TTransactionAbortRequest
    : public TTransactionFinishRequestBase
{
    bool Force;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

struct TTransactionExpirationRequest
{
    void Persist(const NCellMaster::TPersistenceContext& context);
};

using TTransactionFinishRequest = std::variant<
    TTransactionCommitRequest,
    TTransactionAbortRequest,
    TTransactionExpirationRequest>;

////////////////////////////////////////////////////////////////////////////////

//! Ensures that transaction with revoked leases will be finished sometime.
/*!
 *  In common case transaction finish request handler:
 *    1. shcedules leases revocation;
 *    2. waits until leases revocation finished;
 *    3. schedules mutation to finish transaction.
 *  If leader switch happens between these two mutations the master has to take
 *  responsibility of finishing the transaction instead of the client. To
 *  achieve that, transaction finisher persists the origin request during lease
 *  revocation mutation.
 */
struct ITransactionFinisher
    : virtual public TRefCounted
{
    virtual void Initialize() = 0;
    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) = 0;

    //! Prevents transaction finisher from finishing transaction in background
    //! while given request is alive.
    virtual void BeginRequest(
        const NRpc::IServiceContextPtr& context,
        TTransaction* transaction) = 0;

    //! Persistently registers commit/abort request. Must be called in mutation.
    //! If there is already persisted finish request for this transaction and
    //! #update is |false| this method is no-op.
    virtual void PersistRequest(
        TTransaction* transaction,
        const TTransactionFinishRequest& request,
        bool update = false) = 0;

    //! Handles retriable Sequoia error during expired transaction abort.
    virtual void ScheduleExpiredTransactionLeaseRevocation(TTransaction* transaction) = 0;

    //! Decrements active request count for transaction and returns a future
    //! which is set when the transaction is aborted.
    virtual TFuture<void> EndRequestAndGetFailedCommitCompletionFuture(
        NRpc::TRequestId requestId,
        TTransactionId transactionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionFinisher)

////////////////////////////////////////////////////////////////////////////////

ITransactionFinisherPtr CreateTransactionFinisher(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
