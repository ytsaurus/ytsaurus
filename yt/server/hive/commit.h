#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/error.h>

#include <ytlib/transaction_client/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TCommit
{
    DEFINE_BYVAL_RO_PROPERTY(bool, Persistent);
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellGuid>, ParticipantCellGuids);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TCellGuid>, PreparedParticipantCellGuids);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, CommitTimestamp);

public:
    explicit TCommit(const TTransactionId& transactionId);
    TCommit(
        bool persistent,
        const TTransactionId& transactionId,
        const std::vector<TCellGuid>& participantCellGuids);

    TAsyncError GetResult();
    void SetResult(const TError& error);

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

private:
    TPromise<TError> Result_;

    void Init();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
