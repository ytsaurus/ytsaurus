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
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellGuid>, ParticipantCellGuids);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TCellGuid>, PreparedParticipantCellGuids);

public:
    explicit TCommit(const TTransactionId& transactionId);
    TCommit(
        bool persistent,
        const TTransactionId& transactionId,
        const NHydra::TMutationId& mutationId,
        const std::vector<TCellGuid>& participantCellGuids);

    TFuture<TSharedRefArray> GetResult();
    void SetResult(TSharedRefArray result);

    bool IsDistributed() const;

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

private:
    TPromise<TSharedRefArray> Result_;

    void Init();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
