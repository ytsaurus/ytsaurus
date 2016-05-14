#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(TCommit, TTransactionId, ::THash<TTransactionId>)

class TCommit
    : public NHydra::TEntityBase
    , public TRefTracked<TCommit>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellId>, ParticipantCellIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TCellId>, PreparedParticipantCellIds);
    DEFINE_BYVAL_RW_PROPERTY(bool, Persistent);

public:
    explicit TCommit(const TTransactionId& transactionId);
    TCommit(
        const TTransactionId& transactionId,
        const NRpc::TMutationId& mutationId,
        const std::vector<TCellId>& participantCellIds);

    TFuture<TSharedRefArray> GetAsyncResponseMessage();
    void SetResponseMessage(TSharedRefArray message);

    bool IsDistributed() const;

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

private:
    TPromise<TSharedRefArray> ResponseMessagePromise_ = NewPromise<TSharedRefArray>();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
