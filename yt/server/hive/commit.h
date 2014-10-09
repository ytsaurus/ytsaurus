#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/actions/future.h>

#include <ytlib/hydra/public.h>

#include <ytlib/transaction_client/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TCommit
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYVAL_RO_PROPERTY(NHydra::TMutationId, MutationId);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellId>, ParticipantCellIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TCellId>, PreparedParticipantCellIds);

public:
    explicit TCommit(const TTransactionId& transactionId);
    TCommit(
        const TTransactionId& transactionId,
        const NHydra::TMutationId& mutationId,
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
